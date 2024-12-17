import psycopg2
import requests
import csv
import time
import os


# Database connection settings
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'BDP'
DB_USER = 'postgres'
DB_PASSWORD = 'PASSWORD'

# OMDb API settings
OMDB_API_KEY = 'API_KEY'
OMDB_API_URL = 'https://www.omdbapi.com/'

OUTPUT_FILE = 'omdb_api_data.csv'


# Connect to PostgreSQL
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
cur = conn.cursor()

try:
    # Add retrieval_status column if it doesn't already exist
    cur.execute("""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name='netflix_original' AND column_name='retrieval_status'
        ) THEN
            ALTER TABLE public.netflix_original ADD COLUMN retrieval_status TEXT;
        END IF;
    END $$;
    """)
    conn.commit()

    # Query the imdbID column where retrieval_status is NULL
    cur.execute("""
    SELECT const FROM public.netflix_original 
    WHERE const IS NOT NULL AND retrieval_status IS NULL;
    """)
    imdb_ids = cur.fetchall()

    file_exists = os.path.isfile(OUTPUT_FILE)

    # Open CSV file in append mode if it exists, otherwise in write mode
    with open(OUTPUT_FILE, mode='a' if file_exists else 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        # Write the header row only if the file is new
        if not file_exists:
            writer.writerow([
                'imdbID', 'Title', 'Year', 'Rated', 'Released', 'Runtime', 'Genre',
                'Director', 'Writer', 'Actors', 'Plot', 'Language', 'Country', 
                'Awards', 'Poster', 'Ratings', 'Metascore', 'imdbRating', 
                'imdbVotes', 'Type', 'totalSeasons', 'Response'
            ])

        # Loop through imdbID values
        for row in imdb_ids:
            imdb_id = row[0].strip()
            
            if not imdb_id:
                continue

            # Replace spaces in the imdbID if needed
            imdb_id = imdb_id.replace(" ", "+")

            # Make a request to OMDb API
            response = requests.get(OMDB_API_URL, params={'i': imdb_id, 'apikey': OMDB_API_KEY})
            data = response.json()

            # Skip if no valid response
            if data.get('Response') != 'True':
                print(f"Skipping {imdb_id}: {data.get('Error', 'Unknown error')}")
                continue

            # Write data to CSV
            writer.writerow([
                data.get('imdbID', ''),
                data.get('Title', ''),
                data.get('Year', ''),
                data.get('Rated', ''),
                data.get('Released', ''),
                data.get('Runtime', ''),
                data.get('Genre', ''),
                data.get('Director', ''),
                data.get('Writer', ''),
                data.get('Actors', ''),
                data.get('Plot', ''),
                data.get('Language', ''),
                data.get('Country', ''),
                data.get('Awards', ''),
                data.get('Poster', ''),
                str(data.get('Ratings', [])),
                data.get('Metascore', ''),
                data.get('imdbRating', ''),
                data.get('imdbVotes', ''),
                data.get('Type', ''),
                data.get('totalSeasons', ''),
                data.get('Response', '')
            ])

            # Mark as success in the database
            cur.execute("""
            UPDATE public.netflix_original 
            SET retrieval_status = 'success' 
            WHERE const = %s;
            """, (imdb_id,))
            conn.commit()

            print(f"Fetched and marked success for {imdb_id}")

            # Wait for 1 second to avoid rate-limiting
            # time.sleep(1)

finally:
    # Close the database connection
    cur.close()
    conn.close()
