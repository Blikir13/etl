from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'Татаренко Кирилл',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 25),
    'retries': 1,
}

with DAG(
    'flatten_data',
    default_args=default_args,
    description='Преобразование JSON и XML в плоскую структуру',
    schedule_interval=None,
    catchup=False,
    tags=['HSE', 'ETL']
) as dag:
    
    process_json = PostgresOperator(
        task_id='process_json',
        postgres_conn_id='postgres_data',
        sql="""
        
        TRUNCATE TABLE flat_pets;
        
        INSERT INTO flat_pets (name, species, fav_foods, birth_year, photo)
        SELECT 
            pet->>'name' as name,
            pet->>'species' as species,
            CASE 
                WHEN pet->'favFoods' IS NOT NULL 
                THEN array_to_string(
                    ARRAY(
                        SELECT jsonb_array_elements_text(pet->'favFoods')
                    ), 
                    ', '
                )
                ELSE NULL
            END as fav_foods,
            (pet->>'birthYear')::int as birth_year,
            pet->>'photo' as photo
        FROM raw_json,
        LATERAL jsonb_array_elements(json_data->'pets') as pet;
        """
    )
    
    process_xml = PostgresOperator(
        task_id='process_xml',
        postgres_conn_id='postgres_data',
        sql="""
        
        TRUNCATE TABLE flat_foods;
        
        INSERT INTO flat_foods (
            food_name, manufacturer, serving_size,
            calories_total, calories_fat,
            total_fat, saturated_fat, cholesterol, sodium,
            carb, fiber, protein,
            vitamin_a, vitamin_c, calcium, iron
        )
        SELECT 
            (xpath('//name/text()', food_node))[1]::text as food_name,
            (xpath('//mfr/text()', food_node))[1]::text as manufacturer,
            (xpath('//serving/text()', food_node))[1]::text as serving_size,
            NULLIF((xpath('//calories/@total', food_node))[1]::text, '')::int as calories_total,
            NULLIF((xpath('//calories/@fat', food_node))[1]::text, '')::int as calories_fat,
            (xpath('//total-fat/text()', food_node))[1]::text as total_fat,
            (xpath('//saturated-fat/text()', food_node))[1]::text as saturated_fat,
            (xpath('//cholesterol/text()', food_node))[1]::text as cholesterol,
            (xpath('//sodium/text()', food_node))[1]::text as sodium,
            (xpath('//carb/text()', food_node))[1]::text as carb,
            (xpath('//fiber/text()', food_node))[1]::text as fiber,
            (xpath('//protein/text()', food_node))[1]::text as protein,
            COALESCE((xpath('//vitamins/a/text()', food_node))[1]::text, '0') as vitamin_a,
            COALESCE((xpath('//vitamins/c/text()', food_node))[1]::text, '0') as vitamin_c,
            COALESCE((xpath('//minerals/ca/text()', food_node))[1]::text, '0') as calcium,
            COALESCE((xpath('//minerals/fe/text()', food_node))[1]::text, '0') as iron
        FROM raw_xml,
            unnest(xpath('//food', xml_data::xml)) as food_node;
        """
    )
    
    process_json >> process_xml