var services = angular.module('mesos-es-ui.services', []);

services.factory('Cluster', function($resource, config) {
    var URL = 'http://' + config.api.host + ':' + config.api.port + '/cluster';
    return $resource(URL);
});

services.factory('Tasks', function($resource, config) {
    var URL = 'http://' + config.api.host + ':' + config.api.port + '/tasks';
    return $resource(URL);
});
