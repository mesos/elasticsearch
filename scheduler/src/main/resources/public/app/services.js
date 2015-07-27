var services = angular.module('mesos-es-ui.services', []);

services.factory('Cluster', function($resource, $location, config) {
    var URL = $location.protocol() + '://' + $location.host() + ':' + $location.port() + '/v1/cluster';
    return $resource(URL);
});

services.factory('Tasks', function($resource, $location, config) {
    var URL = $location.protocol() + '://' + $location.host() + ':' + $location.port() + '/v1/tasks';
    return $resource(URL);
});
