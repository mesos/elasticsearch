package org.apache.mesos.elasticsearch.common.cli.validators;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.ParameterException;

/**
 * Holds CLI validators
 */
public class CLIValidators {

    /**
     * Abstract class to validate a number.
     * @param <T> A numeric type
     */
    public abstract static class PositiveValue<T> implements IValueValidator<T> {
        @Override
        public void validate(String name, T value) throws ParameterException {
            if (notValid(value)) {
                throw new ParameterException("Parameter " + name + " should be greater than zero (found " + value + ")");
            }
        }

        public abstract Boolean notValid(T value);
    }

    /**
     * Validates a positive number. For type Long
     */
    public static class PositiveLong extends PositiveValue<Long> {
        @Override
        public Boolean notValid(Long value) {
            return value <= 0;
        }
    }

    /**
     * Validates a positive number. For type Double
     */
    public static class PositiveDouble extends PositiveValue<Double> {
        @Override
        public Boolean notValid(Double value) {
            return value <= 0;
        }
    }

    /**
     * Validates a positive number. For type Integer
     */
    public static class PositiveInteger extends PositiveValue<Integer> {
        @Override
        public Boolean notValid(Integer value) {
            return value <= 0;
        }
    }

    /**
     * Ensures that the string is not empty. Will strip spaces.
     */
    public static class NotEmptyString implements IParameterValidator {
        @Override
        public void validate(String name, String value) throws ParameterException {
            if (value.replace(" ", "").isEmpty()) {
                throw new ParameterException("Parameter " + name + " cannot be empty");
            }
        }
    }
}
