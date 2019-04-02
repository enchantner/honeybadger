@Library('powerpony') _

pipeline {
    agent any

    stages {
        stage('Test development version') {
            when {
                not {
                    branch 'master'
                }
            }
            steps {
                sh 'echo "ololo"'
            }
        }
        stage('Build nightly image') {
            when {
                branch 'master'
            }
            steps {
                script {
                    buildPythonPackage()
                }
            }
        }
    }
}
