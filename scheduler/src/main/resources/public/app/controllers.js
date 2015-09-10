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

    // @todo move to config too
    var fetchInterval = 3000; // ms

    /** Cluster info **/
    var fetchClusterConfiguration = function() {
        Cluster.get(function (data) {
            $scope.name = data.name;
            data.configuration.Disk = Math.round(data.configuration.Disk / 1024); // MB to GB
            $scope.configuration = data.configuration;
        });
    };
    fetchClusterConfiguration();
    $interval(fetchClusterConfiguration, fetchInterval);

    /** Tasks monitoring **/
    $scope.tasks = [];
    $scope.nodes = [];
    $scope.statesPercentage = [];

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

controllers.controller('ClusterController', function($scope) {

});

controllers.controller('ScalingController', function($scope, config, Scaling) {
    $scope.scaling = {
        nodes: $scope.$parent.configuration.ElasticsearchNodes,
        result: null
    };
    $scope.scalingSubmit = function() {
        if ($scope.scaling.nodes) {
            var success = function(data) {
                $scope.scaling.result = data;
            }
            var error = function(data) {
                if (data.hasOwnProperty('error')) {
                    $scope.scaling.error = data.error;
                } else {
                    $scope.scaling.error = "Unknown error"
                }
            }
            Scaling.save({to: $scope.scaling.nodes}, {}, success, error);
        }
    };
});

controllers.controller('StatsController', function ($scope, $interval, config, Stats) {

    // chart config template object
    var chartConfig = {
        options: {
            chart: {
                type: 'line',
                height: 250
            },
            plotOptions: {
                area: {
                    fillColor: {
                        linearGradient: {
                            x1: 0,
                            y1: 0,
                            x2: 0,
                            y2: 1
                        },
                        stops: [
                            [0, '#003399'],
                            [1, '#3366AA']
                        ]
                    }
               }
            },
            legend: {
                enabled: false
            },
            exporting: {
                enabled: false
            }
        },
        series: [{
            type: 'area',
            data: []
        }],
        title: {
            text: ''
        },
        loading: false,
        xAxis: {
            type: 'datetime'
        },
        yAxis: {
            title: {
                text: ""
            }
        },
        func: function(chart) {}
    };

    // generate chart config objects from template

    $scope.charts = {
        indices: {},
        shards: {},
        docs: {},
        store: {}
    };

    angular.forEach($scope.charts, function(value, key) {
        $scope.charts[key] = angular.copy(chartConfig);
    });

    // configure charts

    $scope.charts.indices.title.text = "Number of indices";
    $scope.charts.indices.options.plotOptions.area.fillColor.stops = [[0, '#74BD43'], [1, '#74BD43']];
    $scope.charts.indices.series.data = (function() {
        return [];
    }());

    $scope.charts.shards.title.text = "Number of shards";
    $scope.charts.shards.options.plotOptions.area.fillColor.stops = [[0, '#3D9953'], [1, '#3D9953']];
    $scope.charts.shards.series.data = [];

    $scope.charts.docs.title.text = "Number of documents";
    $scope.charts.docs.options.plotOptions.area.fillColor.stops = [[0, '#14CC40'], [1, '#14CC40']];
    $scope.charts.docs.series.data = [];

    $scope.charts.store.title.text = "Data size";
    $scope.charts.store.options.plotOptions.area.fillColor.stops = [[0, '#C340FF'], [1, '#C340FF']];
    $scope.charts.store.series.data = [];

    // updating charts

    var updateChart = function(chart, x, y) {
        var series = $scope.charts[chart].series[0].data;
        series.push({x: x,y: y});
        if (series.length > config.charts.history) {
            $scope.charts[chart].series[0].data = series.slice(series.length - config.charts.history);
        }
    };

    var fetchStats = function() {
        Stats.get({}, function(data) {
            updateChart('docs', data.timestamp, data.indices.docs.count);
            updateChart('indices', data.timestamp, data.indices.count);
            if (data.indices.shards) {
                updateChart('shards', data.timestamp, data.indices.shards.total);
            }
            updateChart('store', data.timestamp, data.indices.store.size_in_bytes);
        });
    };

    fetchStats();
    $interval(fetchStats, config.charts.interval);
});

controllers.controller('TasksController', function ($scope) {

});

controllers.controller('ConfigurationController', function ($scope) {

});

controllers.controller('QueryBrowserController', function ($scope, $http, $location, config, Search) {
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
            var success = function(data) {
                $scope.query.results = data.hits;
            }
            var error = function(data) {
                if (data.hasOwnProperty('error')) {
                    $scope.query.error = data.error;
                } else {
                    $scope.query.error = "Unknown error"
                }
            }
            Search.get({q: $scope.query.string}, success, error);
        }
    };

    $scope.resetQuery = function() {
        $scope.query.error = '';
        $scope.query.string = '';
        $scope.query.results = null;
    };
});