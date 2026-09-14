# Testbed 이미지: ./gradlew installDist 산출물을 JRE 17 위에 배치
# 시작 스크립트는 JAVA_OPTS 환경변수를 그대로 사용합니다 (5.4.2 매니페스트의 JAVA_OPTS)
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY build/install/batch-cluster-testbed/ /app/
EXPOSE 7800 8080
ENTRYPOINT ["/app/bin/batch-cluster-testbed"]
