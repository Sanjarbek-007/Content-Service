CREATE TABLE stories (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title VARCHAR(200) NOT NULL,
    content TEXT NOT NULL,
    location VARCHAR(100),
    author_id UUID NOT NULL,
    likes_count INTEGER DEFAULT 0,
    comments_count INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP WITH TIME ZONE DEFAULT NULL
);


CREATE TABLE story_tags (
    story_id UUID REFERENCES stories(id),
    tag VARCHAR(50),
    PRIMARY KEY (story_id, tag)
);

CREATE TABLE authors (
    id UUID PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    full_name VARCHAR(100) NOT NULL
);
