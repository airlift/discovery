package com.proofpoint.discovery;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import java.lang.annotation.Annotation;
import java.util.Set;

import static java.lang.String.format;
import static org.testng.Assert.fail;

public class ValidationAssertions
{
    private static final Validator VALIDATOR = Validation.buildDefaultValidatorFactory().getValidator();

    public static <T> void assertFailsValidation(T object, String field, String message, Class<? extends Annotation> annotation)
    {
        Set<ConstraintViolation<T>> violations = VALIDATOR.validate(object);

        for (ConstraintViolation<T> violation : violations) {
            if (annotation.isInstance(violation.getConstraintDescriptor().getAnnotation()) &&
                violation.getPropertyPath().toString().equals(field)) {

                if (!violation.getMessage().equals(message)) {
                    fail(format("Expected %s.%s to fail validation for %s with message '%s', but message was '%s'",
                         object.getClass().getName(),
                                field,
                                annotation.getClass().getName(),
                                message,
                                violation.getMessage()));

                }
                return;
            }
        }

        fail(format("Expected %s.%s to fail validation for %s with message '%s'",
             object.getClass().getName(),
                    field,
                    annotation.getClass().getName(),
                    message));
    }
}
