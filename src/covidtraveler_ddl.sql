
CREATE TABLE `country` (
  `country_id` INT AUTO_INCREMENT,
  `country_name` VARCHAR(255),
  `country_code` VARCHAR(255),
  PRIMARY KEY (`country_id`)
);


CREATE TABLE `us_state` (
  `us_state_id` INT AUTO_INCREMENT,
  `us_state_name` VARCHAR(255),
  `us_state_code` VARCHAR(255),
  PRIMARY KEY (`us_state_id`)
);


CREATE TABLE `airport` (
  `airport_id` INT AUTO_INCREMENT,
  `airport_code` VARCHAR(10),
  `airport_name` VARCHAR(255),
  `country_id` INT,
  `state_id` INT,
  PRIMARY KEY (`airport_id`),
  FOREIGN KEY (country_id)
      REFERENCES country(country_id),
  FOREIGN KEY (`state_id`)
      REFERENCES us_state(us_state_id)
);

CREATE TABLE `vaccine_international` (
  `vaccine_id` INT AUTO_INCREMENT,
  `country_id` INT,
  `number_people_vaccinated` INT,
  `Date` date,
  PRIMARY KEY (`vaccine_id`),
  FOREIGN KEY (country_id)
      REFERENCES country(country_id)
);

CREATE TABLE `covid_case_international` (
  `covid_id` INT AUTO_INCREMENT,
  `country_id` INT,
  `number_of_cases` INT,
  `Date` date,
  PRIMARY KEY (`covid_id`),
  FOREIGN KEY (country_id)
      REFERENCES country(country_id)
);

CREATE TABLE `covid_case_us` (
  `covid_id` INT AUTO_INCREMENT,
  `us_state_id` INT,
  `number_of_cases` INT,
  `Date` date,
  PRIMARY KEY (`covid_id`),
  FOREIGN KEY (us_state_id)
      REFERENCES us_state(us_state_id)
);

CREATE TABLE `flight_seats_international` (
  `seat_id` INT AUTO_INCREMENT,
  `country_id` INT,
  `number_seat` INT,
  `Date` date,
  PRIMARY KEY (`seat_id`),
  FOREIGN KEY (country_id)
      REFERENCES country(country_id)
);

CREATE TABLE `flight_international` (
  `flight_id` INT AUTO_INCREMENT,
  `departure_airport_id` INT,
  `destination_airport_id` INT,
  `date` DATE,
  `airline` VARCHAR(255),
  PRIMARY KEY (`flight_id`),
  FOREIGN KEY (departure_airport_id)
      REFERENCES airport(airport_id),
  FOREIGN KEY (destination_airport_id)
      REFERENCES airport(airport_id)
);

CREATE TABLE `flights_us` (
  `flight_id` INT AUTO_INCREMENT,
  `departure_airport_id` INT,
  `destination_airport_id` INT,
  `Date` date,
  `airline` VARCHAR(255),
  PRIMARY KEY (`flight_id`),
  FOREIGN KEY (departure_airport_id)
      REFERENCES airport(airport_id),
  FOREIGN KEY (destination_airport_id)
      REFERENCES airport(airport_id)
);

CREATE TABLE `covid_death_us` (
  `covid_death_state_id` INT AUTO_INCREMENT,
  `us_state_id` INT,
  `number_of_deaths` INT,
  `Date` date,
  PRIMARY KEY (`covid_death_state_id`),
  FOREIGN KEY (covid_death_state_id)
      REFERENCES us_state(us_state_id)
);

CREATE TABLE `vaccine_data_country` (
  `vaccine_id` INT AUTO_INCREMENT,
  `country_id` INT,
  `Date` date,
  PRIMARY KEY (`vaccine_id`),
  FOREIGN KEY (country_id)
    REFERENCES country(country_id)
);

CREATE TABLE `vaccine_us` (
  `vaccine_id` INT AUTO_INCREMENT,
  `us_state_id` INT,
  `number_people_vaccinated` INT,
  `percentage_people_vaccinated` FLOAT,
  `Date` date,
  PRIMARY KEY (`vaccine_id`),
  FOREIGN KEY (us_state_id)
      REFERENCES us_state(us_state_id)
);

CREATE TABLE `covid_death_international` (
  `covid_death_country_id` INT AUTO_INCREMENT,
  `country_id` INT,
  `number_of_deaths` INT,
  `Date` Date,
  PRIMARY KEY (`covid_death_country_id`),
  FOREIGN KEY (country_id)
    REFERENCES country(country_id)
);


