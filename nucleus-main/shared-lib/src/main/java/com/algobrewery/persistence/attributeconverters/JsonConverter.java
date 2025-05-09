package com.algobrewery.persistence.attributeconverters;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.Converter;

@Converter
public abstract class JsonConverter<T> implements jakarta.persistence.AttributeConverter<T, String> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String convertToDatabaseColumn(T attribute) {
        try {
            return mapper.writeValueAsString(attribute);
        } catch (Exception e) {
            //Handle exception
            return null;
        }
    }

    @Override
    public T convertToEntityAttribute(String dbData) {
        try {
            return mapper.readValue(dbData, getTypeReference());
        } catch (Exception e) {
            return null;
        }
    }

    protected abstract TypeReference<T> getTypeReference();
}
