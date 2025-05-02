import datetime
import json
import boto3
import botocore
import os
import time
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
bedrock = boto3.client(
    service_name='bedrock-runtime',
    region_name='us-west-2',
    config=botocore.config.Config(connect_timeout=1000, read_timeout=1000)
)
s3 = boto3.client('s3')

def lambda_handler(event, context):
    topic = event['topic']
    topics = event['topics']
    uuid = event['uuid']
    modelID = event['modelID']
    owner = event['owner']
    index = event['index']
    script = event['script']  # 첫 번째 Lambda에서 전달된 스크립트

    # S3에서 타임스탬프가 포함된 트랜스크립트 가져오기
    bucket_name = os.environ.get("BUCKET_NAME")
    transcript_json = get_transcript_from_s3(bucket_name, uuid)
    
    # 타임스탬프가 포함된 스크립트 생성
    timestamped_script = create_timestamped_script(transcript_json)
    
    extracted_highlight = process_topic(topic, topics, timestamped_script, uuid, modelID, owner, index)

    return { 
        'statusCode': 200,
        'processed_topic': extracted_highlight,
        'body': json.dumps('Finished Highlight Extraction!')
    }

def get_transcript_from_s3(bucket_name, uuid):
    """S3에서 트랜스크립트 JSON 가져오기"""
    try:
        json_object = s3.get_object(Bucket=bucket_name, Key=f'videos/{uuid}/Transcript.json')
        json_content = json.load(json_object['Body'])
        return json_content
    except Exception as e:
        print(f"S3에서 트랜스크립트를 가져오는 중 오류 발생: {str(e)}")
        raise

def create_timestamped_script(transcript_json):
    """타임스탬프가 포함된 스크립트 생성"""
    items = transcript_json['results']['items']
    
    timestamped_script = []
    
    current_sentence = []
    sentence_start_time = None
    last_end_time = None
    
    for item in items:
        if item['type'] == 'pronunciation':
            word = item['alternatives'][0]['content']
            start_time = float(item['start_time'])
            end_time = float(item['end_time'])
            
            if sentence_start_time is None:
                sentence_start_time = start_time
            
            current_sentence.append(word)
            last_end_time = end_time
            
        # 구두점 처리
        elif item['type'] == 'punctuation':
            punctuation = item['alternatives'][0]['content']
            if current_sentence:
                current_sentence[-1] += punctuation
            
            # 문장 종결 구두점인 경우 문장 완성
            if punctuation in ['.', '?', '!']:
                sentence_text = ' '.join(current_sentence)
                timestamped_script.append({
                    'text': sentence_text,
                    'start_time': sentence_start_time,
                    'end_time': last_end_time
                })
                current_sentence = []
                sentence_start_time = None
    
    # 남은 문장 처리
    if current_sentence and sentence_start_time is not None:
        sentence_text = ' '.join(current_sentence)
        timestamped_script.append({
            'text': sentence_text,
            'start_time': sentence_start_time,
            'end_time': last_end_time
        })
    
    return timestamped_script
    
def process_topic(topic, topics, timestamped_script, uuid, modelID, owner, index):
    shorts = dynamodb.Table(os.environ["HIGHLIGHT_TABLE_NAME"])
    
    # 타임스탬프가 포함된 스크립트로 섹션 추출 및 처리
    section_data = extract_and_process_section(topic, topics, timestamped_script, modelID)
    
    timestamp = datetime.datetime.now(datetime.UTC).isoformat()[:-6]+"Z"
    
    highlight = {
        "Text": section_data['text'],
        "Question": topic,
        "Index": str(index),
        "VideoName": uuid,
        "createdAt": timestamp,
        "updatedAt": timestamp,
        "owner": owner,
        "timeframes": json.dumps(section_data['timeframes'])  # 타임스탬프 정보 저장
    }
    
    shorts.put_item(Item=highlight)
    
    return section_data

def extract_and_process_section(topic, topics, timestamped_script, modelID):
    # 타임스탬프가 포함된 스크립트를 문자열로 변환
    script_with_timestamps = json.dumps(timestamped_script)

    prompt = f"""
INPUT:
- Script with timestamps: <timestamped_script> {script_with_timestamps} </timestamped_script>
- All topics: <agendas> {topics} </agendas>
- Target topic: <Topic> {topic} </Topic>

TASK:
Extract sentences from the script that best represent the target topic for a short-form video clip (10-50 seconds, ~20-100 words).

REQUIREMENTS:
- Content must directly relate to the target topic
- Selections must make sense as a standalone clip
- Avoid overlap with other topics in <agendas>
- Preserve exact original text and language
- Include accurate timestamps for each segment

EXAMPLE:
Original script segment:
[
  {{"text": "전기차 시대의 또 다른 승리자.", "start_time": 7.9, "end_time": 9.779}},
  {{"text": "전기차 시대에 어떤 승리자가 있을 것이냐.", "start_time": 10.26, "end_time": 12.449}}
]

Correct extraction:
{{
  "text": "전기차 시대의 또 다른 승리자. 전기차 시대에 어떤 승리자가 있을 것이냐.",
  "timeframes": [
    {{
      "text": "전기차 시대의 또 다른 승리자. 전기차 시대에 어떤 승리자가 있을 것이냐.",
      "start_time": 7.9,
      "end_time": 12.449
    }}
  ]
}}

Note: start_time (7.9) is ALWAYS less than end_time (12.449).

OUTPUT:
<thought>
Brief explanation of your selection rationale
</thought>

<JSON>
{{
"VideoTitle": "Clear, engaging title (max 8 words)",
"text": "Selected content with [...] indicating cuts",
"timeframes": [
  {{
    "text": "Segment text",
    "start_time": start_time_in_seconds,
    "end_time": end_time_in_seconds
  }}
]
}}
</JSON>

IMPORTANT:
- Preserve exact wording for timestamp matching
- Use [...] only between non-consecutive selections
- Include accurate start_time and end_time for each segment
- Double check that ALL start_time values are LESS THAN their corresponding end_time values
- Only use timestamps that exist in the original script
- When combining consecutive segments, use the start_time of the first segment and end_time of the last segment
"""

    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 4096,
        "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
        "temperature": 0,
        "top_p": 0
    })

    # 지수 백오프 재시도 로직
    max_retries = 30
    max_backoff = 16
    retry_count = 0

    while True:
        try:
            response = bedrock.invoke_model(body=body, accept='*/*', contentType='application/json', modelId=modelID)
            response_body = json.loads(response['body'].read())
            response_text = response_body['content'][0]['text']
            
            print(response_text)
            
            firstIndex = int(response_text.find('{'))
            endIndex = int(response_text.rfind('}'))
            
            chunk = json.loads(response_text[firstIndex:endIndex+1])
            print("result: ", chunk)
            
            # 타임스탬프 정보가 포함된 결과 반환
            return {
                'text': chunk['text'],
                'timeframes': chunk.get('timeframes', []),
                'VideoTitle': chunk.get('VideoTitle', '')
            }
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ThrottlingException' and retry_count < max_retries:
                retry_count += 1
                backoff_time = min(2 ** (retry_count - 1), max_backoff)
                sleep_time = backoff_time                
                print(f"ThrottlingException 발생. {sleep_time}초 후 재시도 ({retry_count}/{max_retries})...")
                time.sleep(sleep_time)
            else:
                print(f"오류 발생: {str(e)}")
                raise