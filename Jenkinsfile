def label = "maven-${UUID.randomUUID().toString()}"

podTemplate(label: label, containers: [
  containerTemplate(name: 'maven', image: 'maven:3-jdk-8', ttyEnabled: true, command: 'cat')
  ])
{
    node(label) {
        container('maven') {
            stage('Checkout') {
                checkout scm
            }
            ansiColor('xterm') {
                // TODO Add persistentVolumeClaim to greatly speed this up
                stage('Maven install') {
                  sh 'mvn install -Dmaven.test.skip=true'
                }
                stage('Integration Test') {
                  sh 'mvn integration-test'
                }
            }
        }
    }
}
