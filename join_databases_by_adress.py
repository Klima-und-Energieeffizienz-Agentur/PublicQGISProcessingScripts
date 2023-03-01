# -*- coding: utf-8 -*-

"""
***************************************************************************
*                                                                         *
*   This program is free software; you can redistribute it and/or modify  *
*   it under the terms of the GNU General Public License as published by  *
*   the Free Software Foundation; either version 2 of the License, or     *
*   (at your option) any later version.                                   *
*                                                                         *
***************************************************************************
"""

from qgis.PyQt.QtCore import (QCoreApplication,
    QVariant)
from qgis.core import (QgsProcessing,
                       QgsFeatureSink,
                       QgsProcessingUtils,
                       QgsFields,
                       QgsField,
                       QgsProcessingException,
                       QgsProcessingAlgorithm,
                       QgsProcessingParameterFeatureSource,
                       QgsProcessingParameterFeatureSink, 
                       QgsProcessingParameterField)
from qgis import processing


class ExampleProcessingAlgorithm(QgsProcessingAlgorithm):
    # Constants used to refer to parameters and outputs. They will be
    # used when calling the algorithm from another algorithm, or when
    # calling from the QGIS console.

    INPUT_BASE_LAYER = 'INPUT_BASE_LAYER'
    INPUT_JOINED_LAYER = 'INPUT_JOINED_LAYER'
    JOIN_FIELDS = 'JOIN_FIELDS'
    OUTPUT = 'OUTPUT'

    def tr(self, string):
        """
        Returns a translatable string with the self.tr() function.
        """
        return QCoreApplication.translate('Processing', string)

    def createInstance(self):
        return ExampleProcessingAlgorithm()

    def name(self):
        """
        Returns the algorithm name, used for identifying the algorithm. This
        string should be fixed for the algorithm, and must not be localised.
        The name should be unique within each provider. Names should contain
        lowercase alphanumeric characters only and no spaces or other
        formatting characters.
        """
        return 'joinlayersbyaddress'

    def displayName(self):
        """
        Returns the translated algorithm name, which should be used for any
        user-visible display of the algorithm name.
        """
        return self.tr('Join Layers by Address')

    def group(self):
        """
        Returns the name of the group this algorithm belongs to. This string
        should be localised.
        """
        return self.tr('KEEA')

    def groupId(self):
        """
        Returns the unique ID of the group this algorithm belongs to. This
        string should be fixed for the algorithm, and must not be localised.
        The group id should be unique within each provider. Group id should
        contain lowercase alphanumeric characters only and no spaces or other
        formatting characters.
        """
        return 'keea'

    def shortHelpString(self):
        """
        Returns a localised short helper string for the algorithm. This string
        should provide a basic description about what the algorithm does and the
        parameters and outputs associated with it..
        """
        return self.tr("This algorithm takes two databases and joins them by a number of adress fields.")

    def initAlgorithm(self, config=None):
        """
        Here we define the inputs and output of the algorithm, along
        with some other properties.
        """

        # We add the input vector features source. It can have any kind of
        # geometry.
        self.addParameter(
            QgsProcessingParameterFeatureSource(
                self.INPUT_BASE_LAYER,
                self.tr('Input base layer (provides the geometrie)'),
                [QgsProcessing.TypeVectorAnyGeometry]
            )
        )
        """
        self.addParameter(
            QgsProcessingParameterField(
                self.INPUT_FIELD_TOWN,
                self.tr('Field for town name'),
                parentLayerParameterName=self.INPUT_BASE_LAYER
            )
        )
        
        self.addParameter(
            QgsProcessingParameterField(
                self.INPUT_FIELD_STREET,
                self.tr('Field for street name'),
                parentLayerParameterName=self.INPUT_BASE_LAYER
            )
        )

        self.addParameter(
            QgsProcessingParameterField(
                self.INPUT_FIELD_NR,
                self.tr('Field for house number'),
                parentLayerParameterName=self.INPUT_BASE_LAYER
            )
        )

        self.addParameter(
            QgsProcessingParameterField(
                self.INPUT_FIELD_NR,
                self.tr('Field for house number suffix'),
                parentLayerParameterName=self.INPUT_BASE_LAYER,
                optional=True
            )
        )
        """

        self.addParameter(
            QgsProcessingParameterFeatureSource(
                self.INPUT_JOINED_LAYER,
                self.tr('Input joined layer (provides the additional fields added to the base layer)'),
                [QgsProcessing.TypeVectorAnyGeometry]
            )
        )
        
        self.addParameter(
            QgsProcessingParameterField(
                self.JOIN_FIELDS,
                self.tr('Fields to join (leave empty to use all fields)'),
                parentLayerParameterName=self.INPUT_JOINED_LAYER,
                allowMultiple=True, 
                optional=True)
        )

        # We add a feature sink in which to store our processed features (this
        # usually takes the form of a newly created vector layer when the
        # algorithm is run in QGIS).
        self.addParameter(
            QgsProcessingParameterFeatureSink(
                self.OUTPUT,
                self.tr('Output layer')
            )
        )

    def processAlgorithm(self, parameters, context, feedback):
        """
        Here is where the processing itself takes place.
        """

        # Retrieve the feature source and sink. The 'dest_id' variable is used
        # to uniquely identify the feature sink, and must be included in the
        # dictionary returned by the processAlgorithm function.
        source_base_layer = self.parameterAsSource(
            parameters,
            self.INPUT_JOINED_LAYER,
            context
        )
        if source_base_layer is None:
            raise QgsProcessingException(self.invalidSourceError(parameters, self.INPUT_JOINED_LAYER))
            
        source_joined_layer = self.parameterAsSource(
            parameters,
            self.INPUT_JOINED_LAYER,
            context
        )
        if source_joined_layer is None:
            raise QgsProcessingException(self.invalidSourceError(parameters, self.INPUT_JOINED_LAYER))
        
        join_fields = self.parameterAsFields(
            parameters, 
            self.JOIN_FIELDS, 
            context
        )
        if not join_fields:
            # no fields selected, use all
            join_fields = [source_joined_layer.fields().at(i).name() for i in range(len(source_joined_layer.fields()))]
        
        
        source_fields = source_base_layer.fields()
        fields_to_join = QgsFields()
        join_field_indexes = []
            
        def addFieldKeepType(original, stat):
            """
            Adds a field to the output, keeping the same data type as the original
            """
            field = QgsField(original)
            field.setName(field.name() + '_' + stat)
            fields_to_join.append(field)
            
        field_types = []
        for f in join_fields:
            idx = source_joined_layer.fields().lookupField(f)
            if idx >= 0:
                join_field_indexes.append(idx)

                join_field = source_joined_layer.fields().at(idx)
                if join_field.isNumeric():
                    field_types.append('numeric')
                elif join_field.type() in (QVariant.Date, QVariant.Time, QVariant.DateTime):
                    field_types.append('datetime')
                else:
                    field_types.append('string')

                addFieldKeepType(join_field, "joined")
                
        out_fields = QgsProcessingUtils.combineFields(source_fields, fields_to_join)

        (sink, dest_id) = self.parameterAsSink(
            parameters,
            self.OUTPUT,
            context,
            out_fields,
            source_base_layer.wkbType(),
            source_base_layer.sourceCrs()
        )
        if sink is None:
            raise QgsProcessingException(self.invalidSinkError(parameters, self.OUTPUT))
        
        # Send some information to the user
        feedback.pushInfo('CRS is {}'.format(source_base_layer.sourceCrs().authid()))

        # Compute the number of steps to display within the progress bar and
        # get features from source
        total = 100.0 / source_base_layer.featureCount() if source_base_layer.featureCount() else 0
        features = source_base_layer.getFeatures()
        
        def basicSimplifyString(str):
            replace_map = {
                "ä": "ae",
                "ö": "oe",
                "ü": "ue",
                "ß": "ss",
                "-": "",
                " ": "",
                "strasse": "str"
            }
            result = str.lower()
            for key in replace_map:
                result = result.replace(key, replace_map[key])
            return result

        for current, feature in enumerate(features):
            # Stop the algorithm if cancel button has been clicked
            if feedback.isCanceled():
                break

            # Add algo to join here!
            sink.addFeature(feature, QgsFeatureSink.FastInsert)

            # Update the progress bar
            feedback.setProgress(int(current * total))

        # To run another Processing algorithm as part of this algorithm, you can use
        # processing.run(...). Make sure you pass the current context and feedback
        # to processing.run to ensure that all temporary layer outputs are available
        # to the executed algorithm, and that the executed algorithm can send feedback
        # reports to the user (and correctly handle cancellation and progress reports!)
        if False:
            buffered_layer = processing.run("native:buffer", {
                'INPUT': dest_id,
                'DISTANCE': 1.5,
                'SEGMENTS': 5,
                'END_CAP_STYLE': 0,
                'JOIN_STYLE': 0,
                'MITER_LIMIT': 2,
                'DISSOLVE': False,
                'OUTPUT': 'memory:'
            }, context=context, feedback=feedback)['OUTPUT']

        # Return the results of the algorithm. In this case our only result is
        # the feature sink which contains the processed features, but some
        # algorithms may return multiple feature sinks, calculated numeric
        # statistics, etc. These should all be included in the returned
        # dictionary, with keys matching the feature corresponding parameter
        # or output names.
        return {self.OUTPUT: dest_id}
