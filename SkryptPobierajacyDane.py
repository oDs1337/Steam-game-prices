import requests
import pandas as pd
import time
import os

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

COUNTRIES = ['PL', 'US', 'DE', 'FR', 'BR', 'IN', 'CN', 'MX', 'JP',
             'KR', 'CA', 'AU', 'SE', 'CZ', 'HU', 'IT', 'ES', 'NL']

INDICATORS = {
    'PA.NUS.PPP': 'ppp',
    'NY.GDP.PCAP.CD': 'gdp_per_capita',
    'FP.CPI.TOTL.ZG': 'inflation'
}

GAMES = [
    {"id": 1091500, "name": "Cyberpunk_2077"},
]

START_YEAR = 2000
END_YEAR = 2023

def fetch_indicator_data(indicator):
    all_rows = []
    for country in COUNTRIES:
        url = (
            f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"
            f"?format=json&per_page=1000&date={START_YEAR}:{END_YEAR}"
        )
        try:
            response = requests.get(url)
            data = response.json()
            if isinstance(data, list) and len(data) == 2:
                for entry in data[1]:
                    if entry['value'] is not None:
                        all_rows.append({
                            'country': entry['country']['id'],
                            'year': int(entry['date']),
                            INDICATORS[indicator]: entry['value']
                        })
        except Exception as e:
            print(f"Błąd przy pobieraniu {indicator} dla {country}: {e}")
        time.sleep(1)
    return pd.DataFrame(all_rows)

def download_worldbank_data():
    if os.path.exists("worldbank_data.csv"):
        print("worldbank_data.csv już istnieje, pomijam pobieranie.")
        return
    frames = []
    for ind in INDICATORS:
        print(f"Pobieranie: {INDICATORS[ind]}")
        df = fetch_indicator_data(ind)
        frames.append(df)

    df_merged = frames[0]
    for other_df in frames[1:]:
        df_merged = pd.merge(df_merged, other_df, on=['country', 'year'], how='outer')

    df_merged.to_csv("worldbank_data.csv", index=False)
    print("Zapisano worldbank_data.csv")

def get_steam_prices(app_id):
    results = []
    for cc in COUNTRIES:
        try:
            url = f"https://store.steampowered.com/api/appdetails?appids={app_id}&cc={cc}"
            res = requests.get(url, timeout=5).json()
            game_data = res[str(app_id)]
            if game_data.get('success') and 'data' in game_data:
                price_info = game_data['data'].get('price_overview', {})
                final_price = price_info.get('final')
                currency = price_info.get('currency')
                if final_price:
                    results.append({
                        'country': cc.upper(),
                        'price': final_price / 100,
                        'currency': currency
                    })
        except Exception as e:
            print(f"Błąd Steam dla {cc}: {e}")
        time.sleep(1)
    df = pd.DataFrame(results)
    if not df.empty:
        df.to_csv(f"steam_prices_{app_id}.csv", index=False)
    else:
        print(f"Brak danych ze Steam dla AppID {app_id}")
    return df

def run_prediction_model(app_id, game_name):
    spark = SparkSession.builder.appName(f"SteamPricePrediction_{app_id}").getOrCreate()

    macro = spark.read.csv("worldbank_data.csv", header=True, inferSchema=True)

    try:
        steam = pd.read_csv(f"steam_prices_{app_id}.csv")
    except FileNotFoundError:
        print(f"Brak pliku steam_prices_{app_id}.csv")
        return

    if steam.empty:
        print(f"Dane Steam dla {game_name} są puste.")
        return

    steam.columns = [col.lower() for col in steam.columns]
    macro_2023 = macro.filter(macro.year == 2023)
    macro_2023_df = macro_2023.toPandas()
    combined = pd.merge(macro_2023_df, steam, on="country")

    if combined.empty:
        print(f"Nie da się połączyć danych World Bank i Steam dla {game_name}")
        return

    combined['real_price'] = combined['price'] / combined['ppp']
    combined.to_csv(f"combined_train_2023_{game_name}.csv", index=False)

    sdf = spark.createDataFrame(combined)
    assembler = VectorAssembler(inputCols=["ppp", "gdp_per_capita", "inflation"], outputCol="features")
    data = assembler.transform(sdf).select("features", "real_price")

    lr = LinearRegression(featuresCol="features", labelCol="real_price")
    model = lr.fit(data)

    print(f"{game_name}: Model trained.")
    print("  Coefficients:", model.coefficients)
    print("  Intercept:", model.intercept)
    spark.stop()

if __name__ == "__main__":
    download_worldbank_data()

    for game in GAMES:
        print(f"\nPrzetwarzam grę: {game['name']}")
        get_steam_prices(game["id"])
        run_prediction_model(game["id"], game["name"])