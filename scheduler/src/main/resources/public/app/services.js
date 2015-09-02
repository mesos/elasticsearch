var services = angular.module('mesos-es-ui.services', []);

var baseURL = window.location.protocol + '//' + window.location.host + window.location.pathname;

services.factory('Cluster', function($resource, config) {
    var URL = baseURL + 'v1/cluster';
    return $resource(URL);
});

services.factory('Tasks', function($resource, config) {
    var URL = baseURL + 'v1/tasks';
    return $resource(URL);
});

services.factory('Search', function($resource, config) {
    var URL = baseURL + 'v1/es/_search';
    return $resource(URL);
});

services.factory('Stats', function($resource, config) {
    var URL = baseURL + 'v1/es/_cluster/stats';
    return $resource(URL);
});
