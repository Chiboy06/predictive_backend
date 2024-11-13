import json
import boto3
import decimal
from boto3.dynamodb.conditions import Key, Attr
import numpy as np
from sklearn.linear_model import LinearRegression

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

def get_paginated_readings(last_evaluated_key=None, limit=50):
    """Query readings with pagination"""
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table("esp32_v1")
    
    scan_params = {
        'Limit': limit
    }
    
    if last_evaluated_key:
        scan_params['ExclusiveStartKey'] = last_evaluated_key
        
    try:
        response = table.scan(**scan_params)
        items = replace_decimals(response.get('Items', []))
        
        return {
            'items': items,
            'last_evaluated_key': response.get('LastEvaluatedKey'),
            'count': len(items)
        }
    except Exception as e:
        print(f"Error scanning table: {str(e)}")
        return {
            'items': [],
            'error': str(e),
            'count': 0
        }

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
        # Check if we want all readings or a specific timestamp
        if event.get('queryType') == 'all':
            # Get pagination parameters from the event
            last_evaluated_key = event.get('lastEvaluatedKey')
            limit = min(int(event.get('limit', 100)), 100)  # Cap at 100 items
            
            result = get_paginated_readings(
                last_evaluated_key=last_evaluated_key,
                limit=limit
            )

            # Perform forecasting if there's enough data
            if len(result['items']) >= 2:  # Need at least two readings for regression
                power_data = [item['payload']['power1'] for item in result['items']]
                timestamps = [int(item['timestamp']) for item in result['items']]
                result['forecasts'] = forecast_power(power_data, timestamps)

        else:
            # Original single item query
            dynamodb = boto3.resource('dynamodb')
            table = dynamodb.Table("esp32_v1")
            response = table.get_item(
                Key = {
                    "timestamp": event['timestamp']
                }
            )
            result = response.get('Item', {})
            result = replace_decimals(result)

        # Add the following line to return a 200 status code
        return {
            'statusCode': 200,
            'body': json.dumps(result),
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