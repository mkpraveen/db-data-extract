package org.praveenmk.batch.learn.dbdataextractor.processor;

import org.praveenmk.batch.learn.dbdataextractor.model.UserDetails;
import org.springframework.batch.item.ItemProcessor;

public class UserDetailsItemProcessor implements ItemProcessor<UserDetails, UserDetails> {

    @Override
    public UserDetails process(UserDetails user) throws Exception {
        return user;
    }

}
