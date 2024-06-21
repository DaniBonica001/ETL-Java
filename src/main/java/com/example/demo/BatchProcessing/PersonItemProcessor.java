package com.example.demo.BatchProcessing;

import com.example.demo.DTO.Person;
import org.springframework.batch.item.ItemProcessor;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PersonItemProcessor implements ItemProcessor<Person, Person> {

    private static final String EMAIL_REGEX = "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$";
    private static final Pattern EMAIL_PATTERN = Pattern.compile(EMAIL_REGEX);

    @Override
    public Person process(Person item) throws Exception {
        //nombres a mayuscula y valide los emails

        String upperName = item.name().toUpperCase();
        boolean validEmail = isValidEmail(item.email());

        if (!validEmail) {
            System.out.println("Invalid email: " + item.email());
            return new Person(item.id(), upperName, "invalid email");
        }
        return new Person(item.id(), upperName, item.email());
    }

    public boolean isValidEmail(String email) {
        if (email == null) {
            return false;
        }
        Matcher matcher = EMAIL_PATTERN.matcher(email);
        return matcher.matches();
    }
}
