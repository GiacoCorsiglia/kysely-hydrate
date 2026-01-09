-- Insert users in RANDOMIZED order (not alphabetical by username, not sequential by id)
INSERT INTO users (username, email) VALUES
  ('grace', 'grace@example.com'),      -- id 1
  ('alice', 'alice@example.com'),      -- id 2
  ('ivan', 'ivan@example.com'),        -- id 3
  ('eve', 'eve@example.com'),          -- id 4
  ('carol', 'carol@example.com'),      -- id 5
  ('bob', 'bob@example.com'),          -- id 6
  ('judy', 'judy@example.com'),        -- id 7
  ('frank', 'frank@example.com'),      -- id 8
  ('dave', 'dave@example.com'),        -- id 9
  ('heidi', 'heidi@example.com');      -- id 10

-- Insert posts in RANDOMIZED order (not by post id, not by user_id)
INSERT INTO posts (user_id, title, content) VALUES
  (5, 'Post Alpha', 'Content for post alpha'),        -- id 1, user carol
  (9, 'Post Beta', 'Content for post beta'),          -- id 2, user dave
  (6, 'Post Gamma', 'Content for post gamma'),        -- id 3, user bob
  (5, 'Post Delta', 'Content for post delta'),        -- id 4, user carol
  (9, 'Post Epsilon', 'Content for post epsilon'),    -- id 5, user dave
  (6, 'Post Zeta', 'Content for post zeta'),          -- id 6, user bob
  (4, 'Post Eta', 'Content for post eta'),            -- id 7, user eve
  (6, 'Post Theta', 'Content for post theta'),        -- id 8, user bob
  (4, 'Post Iota', 'Content for post iota'),          -- id 9, user eve
  (6, 'Post Kappa', 'Content for post kappa');        -- id 10, user bob

-- Insert comments in RANDOMIZED order
INSERT INTO comments (post_id, user_id, content) VALUES
  (3, 5, 'Comment on gamma by carol'),    -- id 1
  (1, 9, 'Comment on alpha by dave'),     -- id 2
  (6, 4, 'Comment on zeta by eve'),       -- id 3
  (2, 6, 'Comment on beta by bob'),       -- id 4
  (8, 5, 'Comment on theta by carol'),    -- id 5
  (4, 9, 'Comment on delta by dave'),     -- id 6
  (10, 4, 'Comment on kappa by eve'),     -- id 7
  (5, 6, 'Comment on epsilon by bob');    -- id 8

-- Insert profiles in RANDOMIZED order (not by user_id)
INSERT INTO profiles (user_id, bio, avatar_url) VALUES
  (7, 'Bio for judy', 'https://example.com/avatars/7.png'),
  (2, 'Bio for alice', 'https://example.com/avatars/2.png'),
  (9, 'Bio for dave', 'https://example.com/avatars/9.png'),
  (4, 'Bio for eve', 'https://example.com/avatars/4.png'),
  (1, 'Bio for grace', 'https://example.com/avatars/1.png'),
  (6, 'Bio for bob', 'https://example.com/avatars/6.png'),
  (10, 'Bio for heidi', 'https://example.com/avatars/10.png'),
  (3, 'Bio for ivan', 'https://example.com/avatars/3.png'),
  (5, 'Bio for carol', 'https://example.com/avatars/5.png'),
  (8, 'Bio for frank', 'https://example.com/avatars/8.png');

-- Insert replies in RANDOMIZED order (not by id, comment_id, or user_id)
INSERT INTO replies (comment_id, user_id, content) VALUES
  (3, 6, 'Reply to zeta comment by bob'),      -- id 1
  (1, 9, 'Reply to gamma comment by dave'),    -- id 2
  (5, 4, 'Reply to theta comment by eve'),     -- id 3
  (1, 4, 'Another reply to gamma by eve'),     -- id 4
  (3, 5, 'Reply to zeta comment by carol'),    -- id 5
  (6, 6, 'Reply to delta comment by bob');     -- id 6
