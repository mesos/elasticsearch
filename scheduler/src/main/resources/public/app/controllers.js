var controllers = angular.module('mesos-es-ui.controllers', []);

controllers.controller('MainController', function($scope, $interval, config, Tasks) {
    $scope.header = "";

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

    /** Tasks monitoring **/
    $scope.tasks = [];
    $scope.nodesIpAddresses = [];
    $scope.statesPercentage = [];
    var fetchInterval = 5000; // ms

    $scope.taskStatesMapping = {
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
        var ips = [];
        var states = {};
        angular.forEach(data, function(value, key) {
            ips.push(value.http_address);
            states.hasOwnProperty(value.state) || (states[value.state] = []);
            states[value.state].push(value.http_address);
        });
        $scope.nodesIpAddresses = ips;
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

controllers.controller('ClusterController', function($scope, $http, config, Cluster) {
    $scope.$parent.header = 'Elasticsearch Cluster';
    var fetchClusterConfiguration = function() {
        Cluster.get(function (data) {
            $scope.name = data.name;
            $scope.configuration = data.configuration;
        });
    };
    fetchClusterConfiguration();

    $scope.query = {
        string: '',
        node: '',
        results: [],
    };

    $scope.$parent.$watch('nodesIpAddresses', function(value) {
        if (value.length && $scope.query.node == '') {
            $scope.query.node = value[0];
        }
    });

    var searchUrl = function(host, query) {
        return "http://" + host + "/_search?q=" + query;
    };

    $scope.querySubmit = function() {
        if ($scope.query.node && $scope.query.string) {
            $http.get(searchUrl($scope.query.node, $scope.query.string)).
                success(function(data, status, headers) {
                    console.log(data, status, headers);
                    $scope.searchResults = data;
                }).
                error(function(data, status, headers) {
                    console.log(data, status, headers);
                    // @todo add proper error message
                    alert('Error');
                });
        } else {
            // @todo add proper error message
            alert('Error');
        }
    };
});

controllers.controller('TasksController', function ($scope, $interval, config, Tasks) {
    $scope.$parent.header = 'Elasticsearch Tasks';
});
