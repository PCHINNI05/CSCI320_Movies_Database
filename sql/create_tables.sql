-- ===============================================================
-- CSCI 320 Movie Database — DDL
-- ===============================================================

CREATE TABLE users (
    user_id       SERIAL PRIMARY KEY,
    first_name    VARCHAR(100) NOT NULL,
    last_name     VARCHAR(100) NOT NULL,
    username      VARCHAR(50)  NOT NULL UNIQUE,
    email         VARCHAR(150) NOT NULL UNIQUE,
    password      VARCHAR(255) NOT NULL,
    creation_date TIMESTAMP    NOT NULL DEFAULT NOW(),
    last_access_date TIMESTAMP
);

CREATE TABLE collection (
    collection_id   SERIAL PRIMARY KEY,
    collection_name VARCHAR(200) NOT NULL,
    user_id         INT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    creation_date   TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE platform (
    platform_id   SERIAL PRIMARY KEY,
    platform_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE genre (
    genre_id   SERIAL PRIMARY KEY,
    genre_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE studio (
    studio_id SERIAL PRIMARY KEY,
    name      VARCHAR(200) NOT NULL UNIQUE
);

CREATE TABLE employee (
    employee_id SERIAL PRIMARY KEY,
    first_name  VARCHAR(100) NOT NULL,
    last_name   VARCHAR(100) NOT NULL
);

CREATE TABLE movie (
    movie_id    SERIAL PRIMARY KEY,
    title       VARCHAR(300) NOT NULL,
    length      INT          NOT NULL, -- in minutes
    mpaa_rating VARCHAR(10)
        CHECK (mpaa_rating IN ('G','PG','PG-13','R','NC-17','NR'))
);

-- ===============================================================
-- Junction / Relationship Tables
-- ===============================================================

CREATE TABLE follows (
    follower_id INT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    followee_id INT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    PRIMARY KEY (follower_id, followee_id),
    CHECK (follower_id <> followee_id)
);

CREATE TABLE watches (
    user_id    INT       NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    movie_id   INT       NOT NULL REFERENCES movie(movie_id) ON DELETE CASCADE,
    start_time TIMESTAMP NOT NULL DEFAULT NOW(),
    end_time   TIMESTAMP,
    PRIMARY KEY (user_id, movie_id, start_time)
);

CREATE TABLE rates (
    user_id     INT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    movie_id    INT NOT NULL REFERENCES movie(movie_id) ON DELETE CASCADE,
    star_rating INT NOT NULL CHECK (star_rating BETWEEN 1 AND 5),
    PRIMARY KEY (user_id, movie_id)
);

CREATE TABLE collection_contents (
    collection_id INT NOT NULL REFERENCES collection(collection_id) ON DELETE CASCADE,
    movie_id      INT NOT NULL REFERENCES movie(movie_id) ON DELETE CASCADE,
    PRIMARY KEY (collection_id, movie_id)
);

CREATE TABLE has_platform (
    movie_id     INT  NOT NULL REFERENCES movie(movie_id) ON DELETE CASCADE,
    platform_id  INT  NOT NULL REFERENCES platform(platform_id) ON DELETE CASCADE,
    release_date DATE NOT NULL,
    PRIMARY KEY (movie_id, platform_id)
);

CREATE TABLE produces (
    movie_id  INT NOT NULL REFERENCES movie(movie_id) ON DELETE CASCADE,
    studio_id INT NOT NULL REFERENCES studio(studio_id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, studio_id)
);

CREATE TABLE has_genre (
    movie_id INT NOT NULL REFERENCES movie(movie_id) ON DELETE CASCADE,
    genre_id INT NOT NULL REFERENCES genre(genre_id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, genre_id)
);

CREATE TABLE acts_in (
    movie_id    INT NOT NULL REFERENCES movie(movie_id) ON DELETE CASCADE,
    employee_id INT NOT NULL REFERENCES employee(employee_id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, employee_id)
);

CREATE TABLE directs (
    movie_id    INT NOT NULL REFERENCES movie(movie_id) ON DELETE CASCADE,
    employee_id INT NOT NULL REFERENCES employee(employee_id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, employee_id)
);