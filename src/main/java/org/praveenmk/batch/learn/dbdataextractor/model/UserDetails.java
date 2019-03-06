package org.praveenmk.batch.learn.dbdataextractor.model;



public class UserDetails {

    private long userId;
    private String userName;
    private String emailAddress;
    private String phoneNumber;

    public UserDetails(String userName, String emailAddress, String phoneNumber) {
        super();
        this.userName = userName;
        this.emailAddress = emailAddress;
        this.phoneNumber = phoneNumber;
    }

    public UserDetails(long userId, String userName, String emailAddress, String phoneNumber) {
        this.userId = userId;
        this.userName = userName;
        this.emailAddress = emailAddress;
        this.phoneNumber = phoneNumber;
    }

    public UserDetails() {
        super();
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getEmailAddress() {
        return emailAddress;
    }

    public void setEmailAddress(String emailAddress) {
        this.emailAddress = emailAddress;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    @Override
    public String toString() {
        return "UserDetails{" +
                "userId=" + userId +
                ", userName='" + userName + '\'' +
                ", emailAddress='" + emailAddress + '\'' +
                ", phoneNumber='" + phoneNumber + '\'' +
                '}';
    }
}
