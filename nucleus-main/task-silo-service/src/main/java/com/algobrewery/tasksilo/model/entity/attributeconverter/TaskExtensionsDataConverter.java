package com.algobrewery.tasksilo.model.entity.attributeconverter;

import com.algobrewery.persistence.attributeconverters.JsonConverter;
import com.fasterxml.jackson.core.type.TypeReference;

import java.util.Map;

public class TaskExtensionsDataConverter extends JsonConverter<Map<String, Object>> {

    @Override
    protected TypeReference<Map<String, Object>> getTypeReference() {
        return new TypeReference<Map<String, Object>>() {};
    }
}
