
library 'ts-jenkins-shared-library@main'

pipeline {
    agent none
    options {
        copyArtifactPermission('*/TownSuite-Artifact-Publish')
        buildDiscarder(logRotator(numToKeepStr: '10'))
        timestamps()
        timeout(time: 2, unit: 'HOURS')
    }
    environment {
        AUTOCONFIGURE_NUGET = 'true'
        AUTOCONFIGURE_DOCKER = 'true'
    }
    stages {
        stage('Start Automation Script') {
            agent { label 'starting-agent' }
            steps {
                script {
                    townsuite_automation2.start_linux()
                }
            }
        }
        stage('Pipeline') {
            agent { label townsuite_automation2.get_linux_label() }
            stages{
                stage('Clone and Environment Setup') {
                    steps {
                        script {
                            townsuite.common_environment_configuration()
                        }
                    }
                }
                stage('Build') {
                    steps {
                        script {
                            townsuite.common_environment_configuration()
                        }
                        sh '''
                        dotnet build "TownSuite.Dapper.Extras.sln" -p:Platform="Any CPU" -p:Configuration="Release" -p:GeneratePackageOnBuild=false
                        '''
                    }
                }
                stage('Test') {
                    steps {
                        script {
                            townsuite.common_environment_configuration()
                        }
                        sh '''
                        dotnet test "TownSuite.Dapper.Extras.sln" -c "Release" -p:DefineConstants="ENABLE_TESTCONTAINERS" --logger:"nunit"
                        '''
                    }
                }
                stage('Code Sign') {
                    when {
                        expression { return env.BRANCH_NAME.startsWith('PR-') == false }
                    }
                    steps {
                        echo 'Code Signing happening here....'
                        script {
                            townsuite.codesign "${env.WORKSPACE}/TownSuite.DapperExtras/bin", "*TownSuite*.dll", false
                        }
                    }
                }
                stage('Nuget Pack') {
                    steps {
                        sh """
                        mkdir -p "${env.WORKSPACE}/build"
                        dotnet pack TownSuite.Dapper.Extras.sln --no-build -c=Release -p:Platform="Any CPU" --output "${env.WORKSPACE}/build"
                        """
                    }
                }
                stage('Archive') {
                    when {
                        expression { return env.BRANCH_NAME.startsWith('PR-') == false }
                    }
                    steps {
                        echo 'archiving artifacts'
                        script{
                            townsuite.archiveWithRetryAndLock('build/*.nupkg', 3)
                        }
                    }
                }
            }
        }
    }
    post {
        always {
            CleanupVirtualMachines()
        }
        success {
            echo 'Pipeline executed successfully.'
        }
        failure {
            echo 'Pipeline failed.'
        }
        aborted {
            echo 'Pipeline was aborted.'
        }
    }
}

def CleanupVirtualMachines() {
    node('stopping-agent') {
        cleanWs()
        script {
            townsuite_automation2.stop_automation()
        }
    }
}