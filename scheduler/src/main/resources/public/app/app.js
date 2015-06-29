var app = angular.module('mesos-es-ui', [
    'ngRoute',
    'ngResource',
	'mesos-es-ui.config',
	'mesos-es-ui.controllers',
	'mesos-es-ui.directives',
	'mesos-es-ui.services'
]);

app.config(['$routeProvider', function ($routeProvider) {
    $routeProvider.when('/', {
        templateUrl: 'partials/cluster.html',
        controller: 'ClusterController'
    }).when('/tasks', {
        templateUrl: 'partials/tasks.html',
        controller: 'TasksController'
    }).otherwise({
        redirectTo: '/'
    });
}]);
