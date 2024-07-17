CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    full_name VARCHAR(100) NOT NULL,
    bio TEXT,
    countries_visited INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at Bigint DEFAULT 0
                             );

CREATE TABLE IF NOT EXISTS followers (
    follower_id UUID REFERENCES users(id),
    following_id UUID REFERENCES users(id),
    followed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (follower_id, following_id)
    );


CREATE TABLE IF NOT EXISTS stories (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title VARCHAR(200) NOT NULL,
    content TEXT NOT NULL,
    location VARCHAR(100),
    author_id UUID REFERENCES users(id),
    likes_count INTEGER DEFAULT 0,
    comments_count INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at BIGINT Default 0
                             );

CREATE TABLE IF NOT EXISTS story_tags (
    story_id UUID REFERENCES stories(id),
    tag VARCHAR(50),
    PRIMARY KEY (story_id, tag)
);



CREATE TABLE IF NOT EXISTS comments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    content TEXT NOT NULL,
    author_id UUID REFERENCES users(id),
    story_id UUID REFERENCES stories(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                             );

CREATE TABLE IF NOT EXISTS likes (
    user_id UUID REFERENCES users(id),
    story_id UUID REFERENCES stories(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, story_id)
    );

CREATE TABLE IF NOT EXISTS itineraries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title VARCHAR NOT NULL,
    description TEXT,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    author_id UUID NOT NULL,
    likes_count INTEGER DEFAULT 0,
    comments_count INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at BIGINT Default 0
);

CREATE TABLE IF NOT EXISTS itinerary_destinations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    itinerary_id UUID REFERENCES itineraries(id),
    name VARCHAR(100) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL
    );

CREATE TABLE IF NOT EXISTS itinerary_activities (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    destination_id UUID REFERENCES itinerary_destinations(id),
    activity TEXT NOT NULL
    );

CREATE TABLE IF NOT EXISTS comment (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    content TEXT NOT NULL,
    author_id UUID REFERENCES users(id),
    itinerary_id UUID REFERENCES itineraries(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS destinations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    description TEXT,
    best_time_to_visit VARCHAR(100),
    average_cost_per_day DECIMAL(10, 2),
    popularity_score int,
    currency VARCHAR(3),
    language VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at BIGINT Default 0
                             );

CREATE TABLE IF NOT EXISTS messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sender_id UUID REFERENCES users(id),
    recipient_id UUID REFERENCES users(id),
    content TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS travel_tips (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title VARCHAR(200) NOT NULL,
    content TEXT NOT NULL,
    category VARCHAR(50),
    author_id UUID REFERENCES users(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                             );
