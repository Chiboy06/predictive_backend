import json
import boto3
import decimal
import numpy as np
from sklearn.linear_model import LinearRegression
import requests

def replace_decimals(obj):
    if isinstance(obj, list):
        for i in range(len(obj)):
            obj[i] = replace_decimals(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k in obj.keys():
            obj[k] = replace_decimals(obj[k])
        return obj
    elif isinstance(obj, decimal.Decimal):
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    else:
        return obj

def forecast_power(power_data, timestamps):
    """Forecasts future power readings using linear regression."""
    # Prepare data for linear regression
    X = np.array(timestamps).reshape(-1, 1)
    y = np.array(power_data)

    # Fit a linear regression model
    model = LinearRegression()
    model.fit(X, y)

    # Forecast future power values
    forecast_timestamps = [timestamps[-1] + i for i in range(1, 6)]
    X_forecast = np.array(forecast_timestamps).reshape(-1, 1)
    y_forecast = model.predict(X_forecast)

    return {
        "forecasts": [
            {"timestamp": timestamp, "power1": power}
            for timestamp, power in zip(forecast_timestamps, y_forecast)
        ]
    }

def lambda_handler(event, context):
    try:
        # Retrieve data from the provided API endpoint
        response = requests.get("https://bmk5jggu7c.execute-api.us-east-1.amazonaws.com/prod/energy_readings?query=all")
        data = replace_decimals(response.json())

        # Perform forecasting if there's enough data
        if len(data) >= 2:  # Need at least two readings for regression
            power_data = [item['payload']['power1'] for item in data]
            timestamps = [int(item['timestamp']) for item in data]
            forecasts = forecast_power(power_data, timestamps)
            data.extend(forecasts['forecasts'])

        # Return the response
        return {
            'statusCode': 200,
            'body': json.dumps(data),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            }),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }