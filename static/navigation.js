/*
 * Copyright (c) 2016, DDN Storage Corporation.
 */
/*
 *
 * JavaScript library to show the html
 *
 * Author: Li Xi <lixi@ddn.com>
 */

var NAVIGATION = {
    ID_NAV: "#nav",
    KEY_QOS: "QoS"
};

function Navigation(lime)
{
    this.na_lime = lime;
}

Navigation.prototype.na_get_source = function()
{
    var private_data = new NavigationData(NAVIGATION.KEY_QOS,
            this.na_lime);
    var nav_source = [
        {
            title: NAVIGATION.KEY_QOS,
            key: NAVIGATION.KEY_QOS,
            icon: "ui-icon ui-icon-folder-collapsed",
            folder: false,
            data: private_data,
        },
    ];
    return nav_source;
}

Navigation.prototype.na_init = function()
{
    glyph_opts = {
        map: {
            expanderClosed: "ui-icon ui-icon-caret-1-e",
            expanderOpen: "ui-icon ui-icon-caret-1-s",
        }
    };

    var nav_source = this.na_get_source();

    $(NAVIGATION.ID_NAV).fancytree({
        extensions: ["glyph"],
        glyph: glyph_opts,
        source: nav_source,
        activate: function(event, data) {
            var node = data.node;
            node.data.nd_init();
        },
    });
}

Navigation.prototype.na_reload = function()
{
    var tree = $(NAVIGATION.ID_NAV).fancytree("getTree");

    var nav_source = this.na_get_source();
    tree.reload(nav_source);
    if (this.na_active_key != null)
        this.na_activate_key(this.na_active_key);
}

Navigation.prototype.na_activate_key = function(activate_key)
{
    var tree = $(NAVIGATION.ID_NAV).fancytree("getTree");
    var node = tree.getNodeByKey(activate_key);
    if (node == null) {
        console.error("failed to get node by key \""+ activate_key + "\"");
    }
    node.setActive(true, {noEvents: true, noFocus: false});
    this.na_active_key = activate_key;
}

function NavigationData(title, lime)
{
    this.nd_lime = lime;
    this.nd_title = title;
}

NavigationData.prototype.nd_init = function()
{
    var group_edit;
    var expression_edit;

    this.nd_lime.l_fini_func();
    if (this.nd_title == NAVIGATION.KEY_QOS) {
        this.nd_lime.l_qos.qos_page_init();
    }
}

