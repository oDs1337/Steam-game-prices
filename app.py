from flask import Flask, request, jsonify
from flask_cors import CORS
import requests

app = Flask(__name__)
CORS(app, origins=['http://localhost:4200'])

COUNTRIES = ['us', 'gb', 'de', 'fr', 'pl', 'ru', 'br', 'au', 'jp', 'kr', 'ca', 'se', 'no', 'cz', 'hu', 'cn', 'it', 'es', 'nl', 'mx']

def get_ppp(country_code):
    try:
        url = f"https://api.worldbank.org/v2/country/{country_code.upper()}/indicator/PA.NUS.PPP?format=json&per_page=1"
        res = requests.get(url, timeout=5)
        data = res.json()
        return float(data[1][0]['value'])
    except Exception as e:
        print(f"PPP fetch error for {country_code.upper()}: {e}")
        return None

@app.route('/api/get_prices', methods=['POST'])
def get_prices():
    data = request.json
    app_id = data.get('app_id')
    if not app_id:
        return jsonify({'error': 'No app_id provided'}), 400

    prices = []
    for cc in COUNTRIES:
        try:
            url = f"https://store.steampowered.com/api/appdetails?appids={app_id}&cc={cc}"
            response = requests.get(url, timeout=3)
            res_json = response.json()
            game_data = res_json.get(str(app_id), {})
            if game_data.get('success') and 'data' in game_data:
                price_info = game_data['data'].get('price_overview', {})
                final_price = price_info.get('final')
                if final_price:
                    final_price = final_price / 100
                    ppp = get_ppp(cc)
                    price_adjusted = round(final_price / ppp, 2) if ppp else None

                    prices.append({
                        'country': cc.upper(),
                        'currency': price_info.get('currency'),
                        'price': final_price,
                        'price_ppp_usd': price_adjusted
                    })
        except Exception as e:
            print(f"Error fetching for {cc}: {e}")
            continue

    prices = [p for p in prices if p['price_ppp_usd'] is not None]
    sorted_prices = sorted(prices, key=lambda x: x['price_ppp_usd'])

    return jsonify(sorted_prices)

if __name__ == '__main__':
    app.run(debug=True)