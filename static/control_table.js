/*
 * Copyright (c) 2017, DDN Storage Corporation.
 */
/*
 *
 * JavaScript library to show the html
 *
 * Author: Li Xi <lixi@ddn.com>
 */

var CONTROL_TABLE = {
    NAME_BUTTON_BACKWARD: "button_backward",
    ID_BUTTON_BACKWARD: "#button_backward",
    NAME_BUTTON_ADD: "button_add",
    ID_BUTTON_ADD: "#button_add",
    NAME_BUTTON_REMOVE: "button_remove",
    ID_BUTTON_REMOVE: "#button_remove",
    NAME_BUTTON_UP: "button_up",
    ID_BUTTON_UP: "#button_up",
    NAME_BUTTON_DOWN: "button_down",
    ID_BUTTON_DOWN: "#button_down",
    NAME_BUTTON_FORWARD: "button_forward",
    ID_BUTTON_FORWARD: "#button_forward",
    NAME_BUTTON_COPY: "button_copy",
    ID_BUTTON_COPY: "#button_copy",
    NAME_BUTTON_SAVE: "button_save",
    ID_BUTTON_SAVE: "#button_save",
    NAME_BUTTON_RUN: "button_run",
    ID_BUTTON_RUN: "#button_run",
    NAME_TABLE_CONTROL: "table_control",
    ID_TABLE_CONTROL: "#table_control",
};

function copy_to_clipboard(string) {
    var $temp = $("<textarea>");
    $("body").append($temp);
    $temp.val(string).select();
    document.execCommand("copy");
    $temp.remove();
}

function ControlTable(lime)
{
    this.ct_config = lime.l_config;
    this.ct_config_string_saved = JSON.stringify(this.ct_config, null, 4);
    this.ct_lime = lime;
    this.ct_running = false;
}

ControlTable.prototype.ct_save = function()
{
    var config_string = JSON.stringify(this.ct_config, null, 4);
    var that = this;

    if (this.ct_config_string_saved == config_string)
        return;

    $.ajax({
        type : "POST",
        url : "save",
        data: config_string,
        contentType: 'application/json;charset=UTF-8',
        success: function(result) {
            that.ct_config_string_saved = config_string;
            $(CONTROL_TABLE.ID_BUTTON_SAVE).css("opacity", "0.5");
        }
    });
}

ControlTable.prototype.ct_running_update = function(running)
{
    this.ct_running = running;
    if (running) {
        /* TODO: change icon */
        $(CONTROL_TABLE.ID_BUTTON_RUN).css("opacity", "0.5");
    } else {
        $(CONTROL_TABLE.ID_BUTTON_RUN).css("opacity", "1");
    }
}

ControlTable.prototype.ct_run = function()
{
    var date = new Date();
    var date_string = date.toJSON();
    var json = {};
    json["config"] = this.ct_config
    json["workspace"] = date_string;
    var config_string = JSON.stringify(json, null, 4);
    var that = this;
    if (this.ct_running) {
        return;
    }
    this.ct_running_update(true);

    $.ajax({
        type : "POST",
        url : "run",
        data: config_string,
        contentType: 'application/json;charset=UTF-8',
        success: function(result) {
            that.ct_running_update(false);
        },
        error: function(jqXHR, textStatus, errorThrown) {
            that.ct_running_update(false);
        }
    });

    var result_json = that.ct_lime.l_navigation.na_result_new(date_string);
    var node = $(NAVIGATION.ID_NAV).fancytree("getTree").getNodeByKey(NAVIGATION.KEY_RESULT_LIST);
    node.addChildren(result_json);

    var navigation_data = new NavigationData(NAVIGATION.TITLE_RESULT_CONSOLE,
        that.ct_lime);
    navigation_data.nd_result_title = date_string;
    navigation_data.nd_init();
}

ControlTable.prototype.ct_config_update = function()
{
    /*
     * Check whether the saved config is the same with the one updated.
     * If yes, disable the save button.
     */
    var config_string = JSON.stringify(this.ct_config, null, 4);
    if (this.ct_config_string_saved == config_string) {
        $(CONTROL_TABLE.ID_BUTTON_SAVE).css("opacity", "0.5");
    } else {
        $(CONTROL_TABLE.ID_BUTTON_SAVE).css("opacity", "1");
    }
}

ControlTable.prototype.ct_init = function()
{
    var table_string = '<table id="' + CONTROL_TABLE.NAME_TABLE_CONTROL + '"><tbody><tr>' +
        '<td><button id="' + CONTROL_TABLE.NAME_BUTTON_BACKWARD + '">Back</button></td>' +
        '<td><button id="' + CONTROL_TABLE.NAME_BUTTON_FORWARD + '">Forward</button></td>' +
        '<td><button id="' + CONTROL_TABLE.NAME_BUTTON_ADD + '">Add</button></td>' + 
        '<td><button id="' + CONTROL_TABLE.NAME_BUTTON_REMOVE + '">Remove</button></td>' +
        '<td><button id="' + CONTROL_TABLE.NAME_BUTTON_UP + '">Up</button></td>' +
        '<td><button id="' + CONTROL_TABLE.NAME_BUTTON_DOWN + '">Down</button></td>' +
        '<td><button id="' + CONTROL_TABLE.NAME_BUTTON_SAVE + '">Save</button></td>' +
        '<td><button id="' + CONTROL_TABLE.NAME_BUTTON_COPY + '">Copy</button></td>' +
        '<td><button id="' + CONTROL_TABLE.NAME_BUTTON_RUN + '">Run</button></td>' +
        '</tr></tbody></table>';
    var that = this;
    $(table_string).appendTo("#control");

    $(CONTROL_TABLE.ID_BUTTON_BACKWARD).button({
        showLabel: false,
        icon: "ui-icon-arrowthick-1-w",
    });

    $(CONTROL_TABLE.ID_BUTTON_FORWARD).button({
        showLabel: false,
        icon: "ui-icon-arrowthick-1-e",
    });

    $(CONTROL_TABLE.ID_BUTTON_ADD).button({
        showLabel: false,
        icon: "ui-icon-plus",
    });

    $(CONTROL_TABLE.ID_BUTTON_UP).button({
        showLabel: false,
        icon: "ui-icon-arrowthickstop-1-n",
    });

    $(CONTROL_TABLE.ID_BUTTON_DOWN).button({
        showLabel: false,
        icon: "ui-icon-arrowthickstop-1-s",
    });

    $(CONTROL_TABLE.ID_BUTTON_REMOVE).button({
        showLabel: false,
        icon: "ui-icon-minus",
    });

    $(CONTROL_TABLE.ID_BUTTON_SAVE).button({
        showLabel: false,
        icon: "ui-icon-disk",
    });

    $(CONTROL_TABLE.ID_BUTTON_SAVE).click(function(event)
    {
        that.ct_save();
    });
    $(CONTROL_TABLE.ID_BUTTON_SAVE).css("opacity", "0.5");

    $(CONTROL_TABLE.ID_BUTTON_COPY).button({
        showLabel: false,
        icon: "ui-icon-extlink",
    });

    $(CONTROL_TABLE.ID_BUTTON_COPY).click(function(event)
    {
        var config_string;
        config_string = JSON.stringify(that.ct_config, null, 4);
        copy_to_clipboard(config_string);
    });

    $(CONTROL_TABLE.ID_BUTTON_RUN).button({
        showLabel: false,
        icon: "ui-icon-play",
    });
    $(CONTROL_TABLE.ID_BUTTON_RUN).click(function(event)
    {
        if (this.ct_running) {
            return;
        }
        that.ct_run();
    });
}
