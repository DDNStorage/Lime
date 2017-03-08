/*
 * Copyright (c) 2016, DDN Storage Corporation.
 */
/*
 *
 * JavaScript library to show the html
 *
 * Author: Li Xi <lixi@ddn.com>
 */

var QOS = {
    NAME_JOB_COMMON: "job_",
    ID_JOB_COMMON: "#job_",
    NAME_RATE_COMMON: "rate_",
    ID_RATE_COMMON: "#rate_",
    NAME_PANEL_COMMON: "panel_",
    ID_PANEL_COMMON: "#panel_",
    NAME_TIME_COMMON: "time_",
    ID_TIME_COMMON: "#time_",
    NAME_CONSOLE_CONTAINER: "console_container",
    ID_CONSOLE_CONTAINER: "#console_container",
    NAME_CONSOLE: "console",
    ID_CONSOLE: "#console",
    DEFAULT_RATE_LIMIT: 10000,
};

function Job(job_id, panel_chart, panel_option, time_chart, time_option,
             time_data) {
    this.j_job_id = job_id;
    this.j_panel_chart = panel_chart;
    this.j_panel_option = panel_option;
    this.j_time_chart = time_chart;
    this.j_time_option = time_option;
    this.j_time_data = time_data;
}

function QoS(lime)
{
    this.qos_lime = lime;
    this.qos_job_id_dict = [];
    this.qos_job_index_dict = [];
    this.qos_websocket = null;
}

QoS.prototype.qos_page_init = function()
{
    this.qos_console_init();
    this.qos_lime.l_fini_func = this.qos_page_fini;
    this.qos_lime.l_navigation.na_activate_key(NAVIGATION.KEY_QOS);
};

QoS.prototype.qos_job_time_chart_init = function(id_job, index)
{
    var data = [];

    option = {
        title: {
            text: 'I/O Performance'
        },
        tooltip: {
            trigger: 'axis',
            formatter: function (params) {
                params = params[0];
                return params.name + ": " + params.value[1];
            },
            axisPointer: {
                animation: false
            }
        },
        xAxis: {
            type: 'time',
            splitLine: {
                show: false
            }
        },
        yAxis: {
            type: 'value',
            boundaryGap: [0, '100%'],
            splitLine: {
                show: false
            }
        },
        series: [{
            name: 'rate',
            type: 'line',
            showSymbol: false,
            hoverAnimation: false,
            data: data
        }]
    };

    var chart_name = QOS.NAME_TIME_COMMON + index;
    string = '<div id="' + chart_name + '" class="chart_time"></div>';
    $(string).appendTo(id_job);
    var chart = echarts.init(document.getElementById(chart_name));
    chart.setOption(option, true);
    
    return [chart, option, data];
};

QoS.prototype.qos_job_panel_init = function(id_job, index)
{
    var panel_name = QOS.NAME_PANEL_COMMON + index;
    string = '<div id="' + panel_name + '" class="panel"></div>';
    $(string).appendTo(id_job);
    var chart = echarts.init(document.getElementById(panel_name));
    var option = {
        series: [
            {
                name: 'Write Performance',
                min: 0,
//                max: 120,
//                splitNumber: 12,
                type: 'gauge',
                detail: {
                    formatter:'{value}',
                    textStyle: {
                        fontWeight: 'bolder',
                        fontSize: 10,
                    }
                },
                data: [{value: 0, name: 'MB/s'}],

                axisLine: {
                    lineStyle: {
                        color: [[0.09, 'lime'],[0.82, '#1e90ff'],[1, '#ff4500']],
                        width: 3,
                        shadowBlur: 10
                    }
                },

                axisLabel: {
                    textStyle: {
                        fontSize: 8,
                    }
                },

                axisTick: {
                    length :10,
                    lineStyle: {
                        color: 'auto',
                    }
                },
                splitLine: {
                    length :20,
                    lineStyle: {
                        color: 'auto',
                    }
                },
                title : {
                    textStyle: {
                        fontSize: 10,
                    }
                },
            }
        ]
    };
    chart.setOption(option, true);
    return [chart, option];
};

QoS.prototype.qos_job_init = function(job_id, index)
{
    var that = this;
    if (job_id in this.qos_job_id_dict) {
        console.error("multiple jobs with the same ID", job_id);
        return;
    }

    var name_job = QOS.NAME_JOB_COMMON + index;
    var id_job = QOS.ID_JOB_COMMON + index;
    var name_rate = QOS.NAME_RATE_COMMON + index;
    var id_rate = QOS.ID_RATE_COMMON + index;
    var string = '<div id="' + name_job + '"></div>';
    $(string).appendTo("#content");

    string = '<label>' + job_id + '</label><input id="' + name_rate + '" name="value" value="' + QOS.DEFAULT_RATE_LIMIT + '">';
    $(string).appendTo(id_job);

    $(id_rate).spinner({
        spin: function(event, ui) {
            if (ui.value < 0) {
                $(this).spinner("value", 0);
                return false;
            }
        },
        change: function(event, ui) {
            var input_value = $(this).val();
            var j = this.id.substring(QOS.NAME_RATE_COMMON.length);
            var job_id = that.qos_job_index_dict[j].j_job_id;
            var json_data = {
                job_id: job_id,
                rate: input_value,
            };
            var data_string = JSON.stringify(json_data, null, 4);
            that.qos_websocket.send(data_string);
            console.log(job_id, input_value, that.qos_job_index_dict[j], data_string);
        }
    });

    panel_returns = this.qos_job_panel_init(id_job, index);
    time_returns = this.qos_job_time_chart_init(id_job, index);

    job = new Job(job_id, panel_returns[0], panel_returns[1],
                  time_returns[0], time_returns[1], time_returns[2]);
    this.qos_job_index_dict[index] = job;
    this.qos_job_id_dict[job_id] = job;
};

QoS.prototype.qos_jobs_init = function()
{
    for(var i = 0; i < this.qos_lime.l_config.jobs.length; i++) {
        job_id = this.qos_lime.l_config.jobs[i].job_id;
        this.qos_job_init(job_id, i);
    }
};

QoS.prototype.qos_console_init = function()
{
    if (window.WebSocket === undefined) {
        console.error("WebSocket is not supported, no console");
        return;
    }

    var ws_url = 'ws://'+ window.location.hostname +
        (window.location.port ? ':' + window.location.port : '') +
        '/console_websocket';

    var websocket = new WebSocket(ws_url);
    this.qos_websocket = websocket;
    this.qos_jobs_init();

    string = '<div id="' + QOS.NAME_CONSOLE_CONTAINER +
        '" class="console_container"></div>';
    $(string).appendTo("#content");

    string = '<pre id="' + QOS.NAME_CONSOLE +
        '" class="console"></pre>';
    $(string).appendTo(QOS.ID_CONSOLE_CONTAINER);
    var data_string = JSON.stringify(
        this.qos_lime.l_control_table.ct_config,
        null, 4);

    var workspace = this.rc_result_title;
    var that = this;
    websocket.onopen = function(evt) {
        websocket.send(data_string);
    };
    websocket.onclose = function(evt) {
    };
    websocket.onmessage = function(evt) {
        var message = JSON.parse(evt.data);
        var type = message.type;
        var console_message = JSON.stringify(message) + "\n";
        var string = $(QOS.ID_CONSOLE).text() + console_message;
        $(QOS.ID_CONSOLE).text(string);
        $(QOS.ID_CONSOLE_CONTAINER).scrollTop($(QOS.ID_CONSOLE_CONTAINER)[0].scrollHeight);
        if (type == "datapoint") {
            var rate = message.rate;
            var job_id = message.job_id;
            var timestamp = message.time;
            if (!(job_id in that.qos_job_id_dict)) {
                console.error("unexpected datapoint for job", job_id);
                return;
            }
            job = that.qos_job_id_dict[job_id];
            option = job.j_panel_option;
            option.series[0].data[0].value = Math.round(rate);
            job.j_panel_chart.setOption(option, true);

            millisecond = Math.round(timestamp * 1000);
            console.log(millisecond, rate);
            while (job.j_time_data.length >= 60) {
                job.j_time_data.shift();
            }
            job.j_time_data.push({
                name: millisecond,
                value: [millisecond, Math.round(rate)]
            });

            job.j_time_chart.setOption({
                series: [{
                    data: job.j_time_data
                }]
            });
        } else if (type == "command_result") {
        }
    };
    websocket.onerror = function(evt) {
        console.log("onerror");
    };
};

QoS.prototype.qos_page_fini = function()
{
    $(QOS.ID_CONSOLE_CONTAINER).remove();
};
