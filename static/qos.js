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
    NAME_CONSOLE_CONTAINER: "console_container",
    ID_CONSOLE_CONTAINER: "#console_container",
    NAME_CONSOLE: "console",
    ID_CONSOLE: "#console",
};

function QoS(lime)
{
    this.qos_lime = lime;
}

QoS.prototype.qos_page_init = function()
{
    this.qos_console_init();
    this.qos_lime.l_fini_func = this.qos_page_fini;
    this.qos_lime.l_navigation.na_activate_key(NAVIGATION.KEY_QOS);
}


QoS.prototype.qos_console_init = function()
{
    var string = '<div id="' + QOS.NAME_CONSOLE_CONTAINER +
        '" class="console_container"></div>';
    $(string).appendTo("#content");
    if(window.WebSocket != undefined) {
        string = '<pre id="' + QOS.NAME_CONSOLE +
            '" class="console"></pre>';
        $(string).appendTo(QOS.ID_CONSOLE_CONTAINER);
        var data_string = JSON.stringify(
            this.qos_lime.l_control_table.ct_config,
            null, 4);

        var ws_url = 'ws://'+ window.location.hostname +
            (window.location.port ? ':' + window.location.port : '') +
            '/console_websocket';

        var websocket = new WebSocket(ws_url);
        var workspace = this.rc_result_title;
        websocket.onopen = function(evt) {
            websocket.send(data_string);
        };
        websocket.onclose = function(evt) {
        };
        websocket.onmessage = function(evt) {
            var string = $(QOS.ID_CONSOLE).text() + evt.data;
            $(QOS.ID_CONSOLE).text(string);
            $(QOS.ID_CONSOLE_CONTAINER).scrollTop($(QOS.ID_CONSOLE_CONTAINER)[0].scrollHeight);
        };
        websocket.onerror = function(evt) {
            console.log("onerror");
        };
    } else {
        console.error("WebSocket is not supported, no console");
    }
}

QoS.prototype.qos_page_fini = function()
{
    $(QOS.ID_CONSOLE).remove();
}
