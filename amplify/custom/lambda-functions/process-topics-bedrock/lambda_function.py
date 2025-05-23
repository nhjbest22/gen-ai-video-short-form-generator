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
    # 번호가 매겨진 목록으로 스크립트 전달
    script_numbered = "\n".join([f"{i+1}. \"{item['text']}\"" for i, item in enumerate(timestamped_script)])
    
    prompt = f"""
INPUT:
- Numbered script sentences: <script>
{script_numbered}
</script>
- All topics: <agendas> {topics} </agendas>
- Target topic: <Topic> {topic} </Topic>

TASK:
Select 5-10 sentence numbers from the script that best represent the target topic for a short-form video clip.

CRITICAL REQUIREMENTS:
- Return ONLY the sentence numbers (e.g., 1, 2, 3)
- Selected sentences must be coherent when combined
- Total duration should be 20-80 seconds
- Sentences must directly relate to the topic

EXAMPLE 1 - Consecutive sentences:
If sentences 15, 16, 17, 18, 19, 20 form a complete thought:
CORRECT OUTPUT: [15, 16, 17, 18, 19, 20]

EXAMPLE 2 - Non-consecutive sentences:
If the best sentences are 23, 24, 28, 29, 35, 36:
CORRECT OUTPUT: [23, 24, 28, 29, 35, 36]

OUTPUT:
<thought>
Brief explanation of why these specific sentences best represent the topic
</thought>

<JSON>
{{
"VideoTitle": "Clear, engaging title (max 8 words)",
"selected_numbers": [x, y, z, ...]  // Array of selected sentence numbers
}}
</JSON>
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
            
            print("Bedrock Response:", response_text)
            
            firstIndex = int(response_text.find('{'))
            endIndex = int(response_text.rfind('}'))
            
            chunk = json.loads(response_text[firstIndex:endIndex+1])
            
            # 안전성 검사: selected_numbers 존재 여부 확인
            if 'selected_numbers' not in chunk:
                print("오류: 'selected_numbers'가 응답에 없습니다.")
                raise ValueError("Invalid response format: missing 'selected_numbers'")
            
            # 타입 검사 및 변환
            raw_numbers = chunk['selected_numbers']
            valid_numbers = []
            
            for num in raw_numbers:
                try:
                    # 문자열이면 정수로 변환 시도
                    if isinstance(num, str):
                        num = int(num.strip())
                    else:
                        # 정수가 아닌 경우 정수로 변환
                        num = int(num)
                    
                    # 유효한 범위인지 확인
                    if 1 <= num <= len(timestamped_script):
                        valid_numbers.append(num)
                    else:
                        print(f"경고: 인덱스 범위를 벗어난 번호 무시: {num}")
                except (ValueError, TypeError):
                    print(f"경고: 숫자로 변환할 수 없는 값 무시: {num}")
            
            # 유효한 번호가 없으면 오류 발생
            if not valid_numbers:
                print("오류: 유효한 문장 번호가 없습니다.")
                raise ValueError("No valid sentence numbers found")
            
            # 선택된 번호들을 정렬
            valid_numbers.sort()
            print(f"유효한 문장 번호: {valid_numbers}")
            
            # 선택된 문장들을 결합하여 최종 텍스트 생성
            text_segments = []
            timeframes = []
            current_segment = []
            current_start = None
            current_end = None
            
            for i, num in enumerate(valid_numbers):
                idx = num - 1  # 0-based index로 변환
                sentence = timestamped_script[idx]
                
                # 연속된 문장 확인
                if i > 0 and num != valid_numbers[i-1] + 1:
                    # 비연속 구간 발견 - 현재까지의 세그먼트 저장
                    if current_segment:
                        text_segments.append(" ".join(current_segment))
                        timeframes.append({
                            "text": " ".join(current_segment),
                            "start_time": current_start,
                            "end_time": current_end
                        })
                        current_segment = []
                        current_start = None
                        current_end = None
                
                current_segment.append(sentence['text'])
                if current_start is None or sentence['start_time'] < current_start:
                    current_start = sentence['start_time']
                if current_end is None or sentence['end_time'] > current_end:
                    current_end = sentence['end_time']
            
            # 마지막 세그먼트 처리
            if current_segment:
                text_segments.append(" ".join(current_segment))
                timeframes.append({
                    "text": " ".join(current_segment),
                    "start_time": current_start,
                    "end_time": current_end
                })
            
            # 최종 텍스트 생성 ([...] 구분자 사용)
            final_text = " [...] ".join(text_segments)
            
            result = {
                'text': final_text,
                'timeframes': timeframes,
                'VideoTitle': chunk.get('VideoTitle', '')
            }
            
            print("\nFINAL RESULT WITH TIMEFRAMES:")
            print(json.dumps(result, indent=2, ensure_ascii=False))
            
            return result

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