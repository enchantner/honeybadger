@Library('powerpony') _

pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                echo 'Building..'
                buildPythonPackage
            }
        }
        stage('Test') {
            steps {
                echo 'Testing..'
            }
        }
        stage('Deploy') {
            steps {
                echo 'Deploying....'
            }
        }
    }
}
