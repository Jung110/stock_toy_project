# docker file

#사용할 베이스 도커 이미지
FROM python:3.9.18
#API KEY 값
ENV KEY test

# 해당 디렉토리로 이동
## copy . . 과 같이 쓰면 : 이미지를 빌드한 디렉터리의 모든 파일을 컨테이너의 /usr/src/app 디렉터리로 복사
WORKDIR /usr/src/app
COPY . .

# python pip 설치 후 필수 라이브러리 설치 
RUN python -m pip install --upgrade pip 
RUN pip install -r requirements.txt



CMD python daily.py ${KEY}


