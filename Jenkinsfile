def label = "maven-${UUID.randomUUID().toString()}"

podTemplate(label: label, containers: [
  containerTemplate(name: 'maven', image: 'maven:3-jdk-8', ttyEnabled: true, command: 'cat')
  ])
{
    node(label) {
        container('maven') {
            stage('Checkout') {
                checkout scm
                googleStorageDownload bucketUri: 'gs://monplat-jenkins-artifacts/settings.xml', credentialsId: 'monplat-jenkins', localDirectory: './.mvn/'
            }
            ansiColor('xterm') {
                stage('Maven install') {
                  sh 'mvn install -Dmaven.test.skip=true -s .mvn/settings.xml'
                }
                stage('Integration Test') {
                  sh 'mvn integration-test'
                }
                stage('Deploy snapshot') {
                        sh 'mvn deploy -s .mvn/settings.xml'
                }
            }
        }
    }
}
