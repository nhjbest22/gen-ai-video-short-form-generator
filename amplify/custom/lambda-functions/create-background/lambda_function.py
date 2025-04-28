import json
import boto3
from PIL import Image, ImageFont, ImageDraw
from io import BytesIO

s3 = boto3.client('s3')

def wrap_text(text, width, font):
    """Wrap text naturally without stretching"""
    lines = []
    words = text.split()
    current_line = []
    
    for word in words:
        current_line.append(word)
        test_line = ' '.join(current_line)
        bbox = font.getbbox(test_line)
        line_width = bbox[2] - bbox[0]
        
        if line_width > width:
            if len(current_line) > 1:
                current_line.pop()
                lines.append(' '.join(current_line))
                current_line = [word]
            else:
                lines.append(word)
                current_line = []
    
    if current_line:
        lines.append(' '.join(current_line))
    
    return lines

def load_font(font_path, size):
    try:
        font = ImageFont.truetype(font_path, size)
        return font
    except IOError:
        print("Failed to load the primary font. Falling back to secondary font.")
        return None

def get_text_dimensions(text, font):
    """Get the actual dimensions of the text without stretching"""
    bbox = font.getbbox(text)
    return bbox[2] - bbox[0], bbox[3] - bbox[1]  # width, height

def lambda_handler(event, context):
    bucket_name = event["bucket_name"]   
    uuid = event['videoId']
    index = event['highlight']
    question = event['question']
    vertical = event['inputs'][0]['Vertical']
    
    image_width = 1080
        
    if vertical:
        source_key = 'assets/shorts-background-vertical.png'
        image_height = 1920
        initial_text_size = 60

        title_y_start = 175  # 제목 시작 Y좌표
        title_y_end = 275    # 제목 끝 Y좌표

    else:
        source_key = 'assets/shorts-background-1x1.png'
        image_height = 1920
        initial_text_size = 84

        title_y_start = 200  # 제목 시작 Y좌표
        title_y_end = 300    # 제목 끝 Y좌표


    title_height = title_y_end - title_y_start  # 제목 영역 높이

    
    destination_dir = f'videos/{uuid}/background'
    
    # 기존 코드와 동일: 이미지 로드 및 준비
    response_image = s3.get_object(Bucket=bucket_name, Key=source_key)['Body'].read()
    base_image = Image.open(BytesIO(response_image))
    
    if base_image.size != (image_width, image_height):
        base_image = base_image.resize((image_width, image_height), Image.LANCZOS)
    
    draw = ImageDraw.Draw(base_image)
    
    # 텍스트 구성 설정
    font_path = './NotoSansKR-SemiBold.ttf'
    padding_x = 40
    padding_y = 10  # 패딩을 줄여서 제한된 영역에 더 많은 텍스트 공간 확보
    
    available_width = image_width - (2 * padding_x)
    available_height = title_height - (2 * padding_y)  # 이제 이 높이는 90px (340-250-2*10)
    line_spacing = 8
    
    # 폰트 크기 찾기 로직은 동일하게 유지
    text_size = initial_text_size
    min_text_size = 40
    
    while text_size >= min_text_size:
        font = load_font(font_path, text_size)
        if not font:
            break
            
        lines = wrap_text(question, available_width, font)
        
        if len(lines) > 2:  # 최대 2줄로 제한
            text_size -= 2
            continue
            
        # 총 높이 계산
        total_height = 0
        for line in lines:
            _, height = get_text_dimensions(line, font)
            total_height += height
        
        if len(lines) > 1:
            total_height += line_spacing * (len(lines) - 1)
            
        # 제한된 높이에 맞는지 확인
        if total_height <= available_height:
            break
            
        text_size -= 2
    
    # 제한된 영역 내에서 세로 중앙 정렬 계산
    total_height = 0
    line_heights = []
    for line in lines:
        _, height = get_text_dimensions(line, font)
        line_heights.append(height)
        total_height += height
    
    if len(lines) > 1:
        total_height += line_spacing * (len(lines) - 1)
        
    # 중요 변경: 시작 Y좌표를 title_y_start 기준으로 계산
    current_y = title_y_start + padding_y + (available_height - total_height) / 2
    
    # 텍스트 그리기
    black = (0, 0, 0)
    
    for i, line in enumerate(lines):
        width, _ = get_text_dimensions(line, font)
        text_x = (image_width - width) / 2
        
        draw.text((text_x, current_y), line, font=font, fill=black)
        current_y += line_heights[i] + line_spacing
    
    # 저장 및 업로드 코드는 동일하게 유지
    buffer = BytesIO()
    base_image.save(buffer, format='png')
    buffer.seek(0)
    destination_key = f'{destination_dir}/{index}.png'
    s3.put_object(Bucket=bucket_name, Key=destination_key, Body=buffer, ContentType='image/png')
    
    return {
        'statusCode': 200,
        'body': json.dumps('Created Background Image')
    }
