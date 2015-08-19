var controllers = angular.module('mesos-es-ui.controllers', []);

controllers.controller('MainController', function($scope, $interval, $route, config, Cluster, Tasks) {
    $scope.$route = $route;

    /** Responsiveness helpers **/
    var mobileView = 992;
    $scope.getWidth = function() {
        return window.innerWidth;
    };
    $scope.$watch($scope.getWidth, function(newValue, oldValue) {
        if (newValue >= mobileView) {
            $scope.toggle = true;
        } else {
            $scope.toggle = false;
        }
    });
    $scope.toggleSidebar = function() {
        $scope.toggle = !$scope.toggle;
    };
    window.onresize = function() {
        $scope.$apply();
    };

    /** Cluster info **/
    var fetchClusterConfiguration = function() {
        Cluster.get(function (data) {
            $scope.name = data.name;
            $scope.configuration = data.configuration;
        });
    };
    fetchClusterConfiguration();

    /** Tasks monitoring **/
    $scope.tasks = [];
    $scope.nodes = [];
    $scope.statesPercentage = [];
    var fetchInterval = 5000; // ms

    $scope.taskStatesMapping = {
        TASK_STAGING: {
            progressBarType: "warning"
        },
        TASK_RUNNING: {
            progressBarType: "success"
        },
        TASK_FAILED: {
            progressBarType: "warning"
        },
        TASK_ERROR: {
            progressBarType: "danger"
        },
        TASK_STARTING: {
            progressBarType: "primary"
        },
        TASK_FINISHED: {
            progressBarType: "info"
        },
        TASK_LOST: {
            progressBarType: "danger"
        }
    };

    var updateStatesPercentage = function(states, tasksData) {
        var statesPercentage = [];
        angular.forEach(states, function(value, key) {
            if ($scope.taskStatesMapping[key]) {
                statesPercentage.push({
                    state: key,
                    type: $scope.taskStatesMapping[key].progressBarType,
                    percentage: Math.round(value.length / tasksData.length * 100)
                });
            }
        });
        statesPercentage.sort(function(a, b) {
            return (a.state < b.state) ? 1 : -1;
        });
        $scope.statesPercentage = statesPercentage;
    };
    var updateTasks = function(data) {
        $scope.tasks = data;
        var nodes = [];
        var states = {};
        angular.forEach(data, function(value, key) {
            nodes.push(value.hostname + ":" + value.http_address.split(":")[1]);
            states.hasOwnProperty(value.state) || (states[value.state] = []);
            states[value.state].push(value.http_address);
        });
        $scope.nodes = nodes;
        updateStatesPercentage(states, data);
    };
    var fetchTasks = function() {
        Tasks.query(function (data) {
            updateTasks(data);
        });
    };
    fetchTasks();
    $interval(fetchTasks, fetchInterval);
});

controllers.controller('ClusterController', function($scope, $http, $location, config, Cluster) {
    $scope.query = {
        error: '',
        string: '',
        node: '',
        results: null,
    };

    $scope.$parent.$watch('nodes', function(value) {
        if (value.length && $scope.query.node == '') {
            $scope.query.node = value[0];
        }
    });

    $scope.querySubmit = function() {
        if ($scope.query.node && $scope.query.string) {
            $http.defaults.headers.common['X-ElasticSearch-Host'] = $scope.query.node;
            var URL = $location.protocol() + '://' + $location.host() + ':' + $location.port() + "/es/_search?q=" + $scope.query.string;
            $http.get(URL).success(function(data, status, headers) {
                $scope.query.results = data;
            }).error(function(data, status, headers) {
                if (data.hasOwnProperty('error')) {
                    $scope.query.error = data.error;
                } else {
                    $scope.query.error = "Unknown error"
                }
            });
        }
    };

    $scope.resetQuery = function() {
        $scope.query.error = '';
        $scope.query.string = '';
        $scope.query.results = null;
    };
});

controllers.controller('TasksController', function ($scope, $interval, config, Tasks) {

});
