var controllers = angular.module('mesos-es-ui.controllers', []);

controllers.controller('MainController', function($scope, $interval, config, Tasks) {
    $scope.header = "";

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
});

controllers.controller('ClusterController', function($scope, config, Cluster) {
    $scope.$parent.header = 'Cluster';
    var fetchClusterConfiguration = function() {
        Cluster.get(function (data) {
            $scope.name = data.name;
            $scope.configuration = data.configuration;
        });
    };
    fetchClusterConfiguration();
});

controllers.controller('TasksController', function ($scope, $interval, config, Tasks) {
    $scope.$parent.header = 'Elasticsearch Tasks';
    var fetchInterval = 5000;
    var fetchTasks = function() {
        Tasks.query(function (data) {
            $scope.tasks = data;
        });
    };
    fetchTasks();
    $interval(fetchTasks, fetchInterval);
});
