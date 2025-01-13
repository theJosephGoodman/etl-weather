from airflow import DAG

from airflow.decorators import task
from airflow.utils.dates import days_ago

from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


# широта и долгота (Владикавказ)
LATITUDE = 43.0367 
LONGITUDE = 44.6678 

API_CON_ID = 'open_meteo_api'
POSTGRES_CON_ID = 'postgres_default'


default_args = {
    'owner':'geor_lolaev',
    'start_date':days_ago(1)
    }

with DAG(default_args=default_args, dag_id='weather_dag',
         schedule_interval='@daily',
         catchup=False) as dag:
    
    @task()
    def get_data():
        """Получить данные о погоде по APIшке"""
        
        http_hook = HttpHook(http_conn_id=API_CON_ID, method='GET')
                             
        endpoint = f'v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
        
        response = http_hook.run(endpoint)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f'Ошибка при загрузке данных: {response.status_code}')
        
        
    @task()
    def transform_data(weather_data):
        """Трансформировать данные"""
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }
        return transformed_data
    
    
    @task()
    def load_to_db_data(transformed_data):
        pg_hook = PostgresHook(postgres_con_id = POSTGRES_CON_ID)
        conn = pg_hook.get_conn()
        
        cursor = conn.cursor()
        
        cursor.execute(query="""
                       CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        cursor.execute("""INSERT INTO weather_data(latitude, longitude, temperature, windspeed, winddirection, weathercode)
                       VALUES(%s, %s, %s, %s, %s, %s)
                       """,
                       (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode'])
                       )
        conn.commit()
        cursor.close()
        
        conn.commit()
        cursor.close()
        
    weather_data= get_data()
    transformed_data=transform_data(weather_data)
    load_to_db_data(transformed_data)
        
        