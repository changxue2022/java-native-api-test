# java-native-api-test
Test the java native api of iotdb.
This project is developed by java, maven and TestNG.

## required environment
jdk: >= 1.8
maven: >= 3.8.1

## configuration
modify the iotdb connection:
src/resources/config.properties

## run
```shell
mvn test
```

## report
after running, there would be a directory named surefire-reports
"target/surefire-reports/Default Suite/java-native-api-test.html"

## option
you could choose what test cases by modifying src/resource/testng.xml

