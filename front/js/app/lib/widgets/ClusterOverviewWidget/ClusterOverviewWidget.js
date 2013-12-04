/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var _ = require('underscore');
var kt = require('knights-templar');
var BaseView = DT.widgets.OverviewWidget;
var Notifier = DT.lib.Notifier;
var ClusterMetricsModel = DT.lib.ClusterMetricsModel;

/**
 * This widget displays info about the cluster.
 * 
*/
var ClusterOverviewWidget = BaseView.extend({
    
    initialize: function(options) {
        
        // Set up cluster model
        this.model = new ClusterMetricsModel({}, { dataSource: options.dataSource });
        this.model.fetch();
        this.model.subscribe();

        // super
        BaseView.prototype.initialize.call(this, options);
    },

    events: {
        'click .refreshCluster': 'refreshCluster'
    },

    refreshCluster: function() {
        this.model.fetch({
            success: function() {
                Notifier.success({
                    title: DT.lang('Cluster Info Refreshed'),
                    text: DT.lang('The cluster info has been successfully updated.')
                });
            },
            error: function(xhr, textStatus, errorThrown) {
				Notifier.success({
					title: DT.lang('Error Refreshing Cluster'),
					text: DT.lang('An error occurred refreshing the cluster. Error: ') + errorThrown
				});
            }
        });
    },

    remove: function() {
        this.model.unsubscribe();
        BaseView.prototype.remove.call(this);
    },

    template: kt.make(__dirname+'/ClusterOverviewWidget.html','_')
    
});

exports = module.exports = ClusterOverviewWidget;