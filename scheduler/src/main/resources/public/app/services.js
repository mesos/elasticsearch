var services = angular.module('mesos-es-ui.services', []);

services.factory('Cluster', function($resource, config) {
    var URL = window.location.protocol + '//' + window.location.host + window.location.pathname + 'v1/cluster';
    return $resource(URL);
});

services.factory('Tasks', function($resource, config) {
    var URL = window.location.protocol + '//' + window.location.host + window.location.pathname + 'v1/tasks';
    return $resource(URL);
});
