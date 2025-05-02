import boto3
import json
import os
import logging
from datetime import datetime

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Get environment variables
BUCKET_NAME = os.environ["BUCKET_NAME"]
HIGHLIGHT_TABLE_NAME = os.environ["HIGHLIGHT_TABLE_NAME"]

def convert_seconds_to_timecode(seconds):
    """초를 타임코드 형식(HH:MM:SS:FF)으로 변환"""
    seconds = float(seconds)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    frames = int((seconds - int(seconds)) * 24)  # 24 fps 가정
    return "{:02d}:{:02d}:{:02d}:{:02d}".format(int(hours), int(minutes), int(seconds), frames)

def lambda_handler(event, context):
    try:
        uuid = event['uuid']
        index = str(event['index'])
        
        logger.info(f"Processing request for UUID: {uuid}, Index: {index}")

        shorts_table = dynamodb.Table(HIGHLIGHT_TABLE_NAME)

        raw_file_path = f's3://{BUCKET_NAME}/videos/{uuid}/RAW.mp4'
        output_destination = f's3://{BUCKET_NAME}/videos/{uuid}/FHD/{index}-FHD'

        # DynamoDB에서 하이라이트 정보 가져오기
        response = shorts_table.get_item(Key={'VideoName': uuid, 'Index': index})
        item = response.get('Item')
        if not item:
            logger.error(f"Item not found in DynamoDB for UUID: {uuid}, Index: {index}")
            raise ValueError("Item not found in DynamoDB")

        # 이전 Lambda에서 전달된 timeframes 정보 파싱
        try:
            timeframes_data = json.loads(item.get("timeframes", "[]"))
            
            # timeframes 데이터 형식 확인 및 처리
            timeframes = []
            for frame in timeframes_data:
                start_time = frame.get("start_time")
                end_time = frame.get("end_time")
                if start_time is not None and end_time is not None:
                    timeframes.append((float(start_time), float(end_time)))
            
            # 시작 시간을 기준으로 타임프레임 정렬 (중요!)
            timeframes.sort(key=lambda x: x[0])
            
            logger.info(f"Parsed and sorted {len(timeframes)} timeframes from previous Lambda result")
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in timeframes data for UUID: {uuid}, Index: {index}")
            timeframes = []
        except Exception as e:
            logger.error(f"Error parsing timeframes: {str(e)}")
            timeframes = []

        if not timeframes:
            logger.warning(f"No valid timeframes found for UUID: {uuid}, Index: {index}")
            return {
                'statusCode': 400,
                'body': 'Error on extracting timeframe',
                'success': 'false',
                'index': index,
                'duration': 0,
                'timeframes': [],
                'raw_file_path': raw_file_path,
                'output_destination': output_destination, 
                'uuid': uuid
            }
        
        # 총 재생 시간 계산
        total_duration = int(sum(end - start for start, end in timeframes))
        
        # 타임코드 형식으로 변환 (정렬된 순서 유지)
        formatted_timeframes = [
            {
                "StartTimecode": convert_seconds_to_timecode(start),
                "EndTimecode": convert_seconds_to_timecode(end)
            }
            for start, end in timeframes
        ]

        logger.info(f"Formatted timeframes for UUID: {uuid}, Index: {index}: {formatted_timeframes}, Duration: {total_duration}")

        # DynamoDB 업데이트
        shorts_table.update_item(
            Key={'VideoName': uuid, 'Index': index},
            UpdateExpression='SET #dur = :durVal, #tf = :tfVal',
            ExpressionAttributeNames={'#dur': 'duration', '#tf': 'timeframes'},
            ExpressionAttributeValues={':durVal': total_duration, ':tfVal': json.dumps(formatted_timeframes)}
        )

        logger.info(f"Successfully processed request for UUID: {uuid}, Index: {index}")

        return {
            'statusCode': 200,
            'body': 'Extracted Timeline',
            'success': 'true',
            'index': index,
            'duration': total_duration,
            'uuid': uuid,
            'timeframes': formatted_timeframes,
            'output_destination': output_destination,
            'raw_file_path': raw_file_path
        }

    except Exception as e:
        logger.error(f"An error occurred for UUID: {uuid if 'uuid' in locals() else 'unknown'}, Index: {index if 'index' in locals() else 'unknown'}: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': f'Error processing request: {str(e)}',
            'success': 'false',
            'index': index if 'index' in locals() else 'unknown',
            'uuid': uuid if 'uuid' in locals() else 'unknown',
            'duration': 0,
            'timeframes': [],
            'output_destination': output_destination if 'output_destination' in locals() else '',
            'raw_file_path': raw_file_path if 'raw_file_path' in locals() else ''
        }
