/*
 * Copyright (c) 2016, DDN Storage Corporation.
 */
/*
 *
 * JavaScript library to show the html
 *
 * Author: Li Xi <lixi@ddn.com>
 */

function selectable_select_only_one(container, selecting)
{
    /*
     * Add unselecting class to all elements in the styleboard canvas except
     * the ones to select
     */
    $(".ui-selected", container).not(selecting).removeClass("ui-selected").addClass("ui-unselecting");

    /* Add ui-selecting class to the elements to select */
    $(selecting).not(".ui-selected").addClass("ui-selecting");

    /* Call this otherwise "selectee is undefined" */
    container.selectable('refresh');
    /*
     * trigger the mouse stop event (this will select all .ui-selecting
     * elements, and deselect all .ui-unselecting elements)
     */
    container.data("ui-selectable")._mouseStop(null);
}

function selectable_input_init(input_id, selectee_id, container)
{
    $(input_id).click(function() {
        selectable_select_only_one(container, selectee_id);
    });
    $(input_id).keypress(function(event){
        if (event.which != 13)
            return;

        selectable_select_only_one(container, selectee_id);
    });
}


function table_item_remove(id_tbody, array, name_common, navkey_common)
{
    var removing_names = new Array();
    var new_array = new Array();
    var removing;

    $(id_tbody + ' .ui-selected').each(function() {
        removing_names.push(this.id);
    });

    if (removing_names.length == 0 || removing_names.length != 1)
        return array;

    if (navkey_common != null) {
        var removing_name = removing_names[0];
        var index = removing_name.substring(name_common.length);
        var key = navkey_common + index;
        var node = $(NAVIGATION.ID_NAV).fancytree("getTree").getNodeByKey(key);
        var next_node = node.getNextSibling();
        node.remove();
        while (next_node != null) {
            next_node.key = navkey_common + index;
            index++;
            next_node = next_node.getNextSibling();
        }
    }

    for (var i = 0; i < array.length; i++) {
        item_name = name_common + i;
        removing = false;
        for (var j = 0; j < removing_names.length; j++) {
            if (item_name == removing_names[j]) {
                removing = true;
            }
        }
        if (!removing) {
            new_array.push(array[i]);
        }
    }
    return new_array;
}

/* return the selected index, -1 if no change */
function table_item_move(id_tbody, array, name_common, up)
{
    var selected_names = new Array();
    var removing;

    $(id_tbody + ' .ui-selected').each(function() {
        selected_names.push(this.id);
    });

    if (selected_names.length != 1)
        return -1;

    var selected_name = selected_names[0];
    var selected_index = 0;
    for (selected_index = 0; selected_index < array.length; selected_index++) {
        var item_name = name_common + selected_index;
        if (item_name == selected_name) {
            break;
        }
    }

    if (selected_index == array.length) {
        console.error("invalid selected index");
        return -1;
    }

    if (up) {
        if (selected_index == 0)
            return -1;

        var tmp = array[selected_index - 1];
        array[selected_index - 1] = array[selected_index];
        array[selected_index] = tmp;
        return selected_index - 1;
    }

    if (selected_index == array.length - 1)
        return -1;

    var tmp = array[selected_index + 1];
    array[selected_index + 1] = array[selected_index];
    array[selected_index] = tmp;

    return selected_index + 1;
}

function Lime(config)
{
    this.l_config = config;
    this.l_navigation = null;
    this.l_qos = null;
}

$(document).ready(function ()
{
    var config = $.getJSON("static/lime_config.json");

    $.when(config)
    .done(function(results_config) {
        var lime = new Lime(results_config);

        var qos = new QoS(lime);
        lime.l_qos = qos;

        var control = new ControlTable(lime);
        control.ct_init();
        lime.l_control_table = control;

        var nav = new Navigation(lime);
        lime.l_navigation = nav;

        nav.na_init();
        qos.qos_page_init();
    })
    .fail(function(jqxhr, textStatus, error) {
        console.error("failed to load json files: " + error);
    });
});

