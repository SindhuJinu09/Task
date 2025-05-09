package com.algobrewery.converters;

public interface BiDirectionalConverter<I, O> {
    O doForward(I input);
    I doBackward(O input);
}
