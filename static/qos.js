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
    NAME_RATE_COMMON: "rate_",
    ID_RATE_COMMON: "#rate_",
    NAME_PANEL_COMMON: "panel_",
    ID_PANEL_COMMON: "#panel_",
    NAME_TIME: "time",
    ID_TIME: "#time",
    NAME_CONSOLE_CONTAINER: "console_container",
    ID_CONSOLE_CONTAINER: "#console_container",
    NAME_CONSOLE: "console",
    ID_CONSOLE: "#console",
    DEFAULT_RATE_LIMIT: 10000,
    NAME_JOB_TABLE: "job_table",
    ID_JOB_TABLE: "#job_table",
    NAME_TBODY_JOB: "tbody_job",
    ID_TBODY_JOB: "#tbody_job",
    NAME_JOB_NAME_COMMON: "job_name_",
    ID_JOB_NAME_COMMON: "#job_name_",
    NAME_JOB_PERF_COMMON: "job_perf_",
    ID_JOB_PERF_COMMON: "#job_perf_",
};

function Job(job_id, id_perf) {
    this.j_job_id = job_id;
    this.j_time_data = [];
    this.j_time_series = null;
    this.j_id_perf = id_perf;
}

function QoS(lime)
{
    this.qos_lime = lime;
    this.qos_job_id_dict = [];
    this.qos_job_index_dict = [];
    this.qos_websocket = null;
    this.qos_time_chart = null;
    this.qos_time_option = null;
}

QoS.prototype.qos_page_init = function()
{
    this.qos_console_init();
    this.qos_lime.l_fini_func = this.qos_page_fini;
    this.qos_lime.l_navigation.na_activate_key(NAVIGATION.KEY_QOS);
};

QoS.prototype.qos_job_init = function(job_id, index)
{
    var that = this;
    if (job_id in this.qos_job_id_dict) {
        console.error("multiple jobs with the same ID", job_id);
        return;
    }

    var name_job_name = QOS.NAME_JOB_NAME_COMMON + index;
    var id_job_name = QOS.ID_JOB_NAME_COMMON + index;
    var name_perf = QOS.NAME_JOB_PERF_COMMON + index;
    var id_perf = QOS.ID_JOB_PERF_COMMON + index;
    var name_rate = QOS.NAME_RATE_COMMON + index;
    var id_rate = QOS.ID_RATE_COMMON + index;

    var tr = $("<tr><td><button id='" + name_job_name + "'>" + job_id + "</button></td>" +
        "<td><input id='" + name_rate + "' name='value' value='" +
        QOS.DEFAULT_RATE_LIMIT + "'>" + "</td>" +
        "<td><button id='" + name_perf + "'>0</button></td>" + "</tr>");
    tr.appendTo(QOS.ID_TBODY_JOB);

    $(id_job_name).button();
    $(id_perf).button();

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
        }
    });

    job = new Job(job_id, id_perf);
    this.qos_job_index_dict[index] = job;
    this.qos_job_id_dict[job_id] = job;
    job.j_time_series = {
        name: job_id,
        type: 'line',
        showSymbol: false,
        hoverAnimation: false,
        data: job.j_time_data
    };
    this.qos_time_option.series.push(job.j_time_series);
    this.qos_time_option.legend.data.push(job_id);
};

QoS.prototype.qos_jobs_init = function()
{
    for(var i = 0; i < this.qos_lime.l_config.cluster.jobs.length; i++) {
        job_id = this.qos_lime.l_config.cluster.jobs[i].job_id;
        this.qos_job_init(job_id, i);
    }
};

QoS.prototype.qos_time_chart_init = function()
{
    option = {
        title: {
            text: 'I/O Performance',
            textStyle: {
                fontSize: 12,
            }
        },
        tooltip: {
            trigger: 'axis',
        },
        legend: {
            data: []
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
        series: []
    };

    string = '<div id="' + QOS.NAME_TIME + '" class="chart_time"></div>';
    $(string).appendTo("#content");
    var chart = echarts.init(document.getElementById(QOS.NAME_TIME));
    chart.setOption(option, true);
    this.qos_time_chart = chart;
    this.qos_time_option = option;
};

QoS.prototype.qos_job_table_init = function()
{
    var table_string = '<table id="' + QOS.NAME_JOB_TABLE + '"><tbody id="' +
        QOS.NAME_TBODY_JOB + '"></tbody></table>';
    $(table_string).appendTo("#content");
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
    this.qos_time_chart_init();
    this.qos_job_table_init();
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
            $(job.j_id_perf).html(Math.round(rate));

            millisecond = Math.round(timestamp * 1000);
            while (job.j_time_data.length >= 60) {
                job.j_time_data.shift();
            }
            job.j_time_data.push({
                name: millisecond,
                value: [millisecond, Math.round(rate)]
            });

            that.qos_time_chart.setOption(that.qos_time_option);
        } else if (type == "command_result") {
        }
    };
    websocket.onerror = function(evt) {
        console.log("onerror");
    };
};

QoS.prototype.qos_page_fini = function()
{
    $(QOS.ID_TIME).remove();
    $(QOS.ID_CONSOLE_CONTAINER).remove();
    $(QOS.ID_JOB_TABLE).remove();
};
