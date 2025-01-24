listOfNames = [
    "Alice", "Bob", "Charlie", "Daisy", "Edward", "Fiona", "Grace", "Henry", "Irene", "Jack",
    "Karen", "Liam", "Mia", "Noah", "Olivia", "Paul", "Quinn", "Rachel", "Steve", "Tina",
    "Uma", "Victor", "Wendy", "Xavier", "Yara", "Zach", "Amanda", "Brian", "Chloe", "Daniel",
    "Emily", "Frank", "Gina", "Harry", "Isabelle", "Jacob", "Kate", "Lucas", "Molly", "Nathan",
    "Ophelia", "Peter", "Quinn", "Ruby", "Samuel", "Tessa", "Ulrich", "Vanessa", "Walter", "Xena",
    "Yvonne", "Zoe", "Adam", "Bella", "Caleb", "Diana", "Ethan", "Felicity", "Gavin", "Hazel",
    "Ivan", "Jenna", "Kyle", "Laura", "Marcus", "Nina", "Owen", "Paige", "Riley", "Sarah",
    "Thomas", "Ursula", "Veronica", "William", "Xavier", "Yasmine", "Zara", "Abigail", "Brandon",
    "Clara", "Derek", "Eliza", "Fabian", "Gemma", "Hugo", "Isla", "Jason", "Kylie", "Liam",
    "Melissa", "Noah", "Olivia", "Patrick", "Quentin", "Rose", "Simon", "Taylor", "Ulrika", "Victor",
    "Whitney"
]

products = [
    # Electronics
    "Smartphone", "Laptop", "Tablet", "Desktop PC", "Smartwatch", "Bluetooth Headphones",
    "Wireless Earbuds", "Gaming Console", "Digital Camera", "Action Camera", "Power Bank",
    "Smart Speaker", "Router", "Monitor", "External Hard Drive", "Keyboard", "Mouse",
    "Graphics Card", "Printer", "Drone",

    # Clothing
    "T-Shirt", "Jeans", "Hoodie", "Jacket", "Formal Shirt", "Skirt", "Dress", "Blouse",
    "Shorts", "Sweater", "Tracksuit", "Socks", "Hat", "Gloves", "Scarf", "Suit", "Tie",
    "Belt", "Sportswear", "Pajamas",

    # Home Appliances
    "Refrigerator", "Washing Machine", "Microwave Oven", "Air Conditioner", "Vacuum Cleaner",
    "Toaster", "Blender", "Electric Kettle", "Coffee Maker", "Dishwasher", "Rice Cooker",
    "Ceiling Fan", "Water Heater", "Induction Cooktop", "Mixer Grinder", "Air Purifier",
    "Juicer", "Iron", "Slow Cooker", "Food Processor",

    # Books
    "Fiction Novel", "Science Fiction", "Mystery Book", "Thriller Book", "Biography",
    "Self-Help Book", "Textbook", "Children's Book", "Fantasy Book", "History Book",
    "Cookbook", "Travel Guide", "Graphic Novel", "Poetry Book", "Philosophy Book",
    "Psychology Book", "Business Book", "Health and Fitness Book", "DIY Guide", "Encyclopedia",

    # Sports
    "Soccer Ball", "Basketball", "Tennis Racket", "Badminton Racket", "Cricket Bat",
    "Baseball Glove", "Hockey Stick", "Table Tennis Paddle", "Golf Club", "Yoga Mat",
    "Dumbbells"]

categories = {
    
        "electronics":
    ["Smartphone", "Laptop", "Tablet", "Desktop PC", "Smartwatch", "Bluetooth Headphones", 
    "Wireless Earbuds", "Gaming Console", "Digital Camera", "Action Camera", "Power Bank", 
    "Smart Speaker", "Router", "Monitor", "External Hard Drive", "Keyboard", "Mouse", 
    "Graphics Card", "Printer", "Drone"],

    "clothing":
    ["T-Shirt", "Jeans", "Hoodie", "Jacket", "Formal Shirt", "Skirt", "Dress", "Blouse", 
    "Shorts", "Sweater", "Tracksuit", "Socks", "Hat", "Gloves", "Scarf", "Suit", "Tie", 
    "Belt", "Sportswear", "Pajamas"],

    
    "home appliances":
    ["Refrigerator", "Washing Machine", "Microwave Oven", "Air Conditioner", "Vacuum Cleaner", 
    "Toaster", "Blender", "Electric Kettle", "Coffee Maker", "Dishwasher", "Rice Cooker", 
    "Ceiling Fan", "Water Heater", "Induction Cooktop", "Mixer Grinder", "Air Purifier", 
    "Juicer", "Iron", "Slow Cooker", "Food Processor"],

"books":
    ["Fiction Novel", "Science Fiction", "Mystery Book", "Thriller Book", "Biography", 
    "Self-Help Book", "Textbook", "Children's Book", "Fantasy Book", "History Book", 
    "Cookbook", "Travel Guide", "Graphic Novel", "Poetry Book", "Philosophy Book", 
    "Psychology Book", "Business Book", "Health and Fitness Book", "DIY Guide", "Encyclopedia"],

    "sports": ["Soccer Ball", "Basketball", "Tennis Racket", "Badminton Racket", "Cricket Bat", 
    "Baseball Glove", "Hockey Stick", "Table Tennis Paddle", "Golf Club", "Yoga Mat", 
    "Dumbbells", "Treadmill", "Exercise Bike", "Skipping Rope", "Swimming Goggles", 
    "Sports Shoes", "Running Shorts", "Gym Bag", "Resistance Bands", "Boxing Gloves"]
    }

prices = [
    ('Smartphone', 500),
    ('Laptop', 700),
    ('Tablet', 550),
    ('Desktop PC', 800),
    ('Smartwatch', 200),
    ('Bluetooth Headphones', 214),
    ('Wireless Earbuds', 300),
    ('Gaming Console', 200),
    ('Digital Camera', 300),
    ('Action Camera', 200),
    ('Power Bank', 100),
    ('Smart Speaker', 300),
    ('Router', 100),
    ('Monitor', 250),
    ('External Hard Drive', 80),
    ('Keyboard', 90),
    ('Mouse', 60),
    ('Graphics Card', 280),
    ('Printer', 300),
    ('Drone', 316),
    ('T-Shirt', 28),
    ('Jeans', 70),
    ('Hoodie', 23),
    ('Jacket', 104),
    ('Formal Shirt', 129),
    ('Skirt', 20),
    ('Dress', 45),
    ('Blouse', 25),
    ('Shorts', 46),
    ('Sweater', 37),
    ('Tracksuit', 80),
    ('Socks', 6),
    ('Hat', 20),
    ('Gloves', 10),
    ('Scarf', 30),
    ('Suit', 218),
    ('Tie', 25),
    ('Belt', 55),
    ('Sportswear', 12),
    ('Pajamas', 18),
    ('Refrigerator', 500),
    ('Washing Machine', 400),
    ('Microwave Oven', 52),
    ('Air Conditioner', 76),
    ('Vacuum Cleaner', 48),
    ('Toaster', 248),
    ('Blender', 161),
    ('Electric Kettle', 67),
    ('Coffee Maker', 45),
    ('Dishwasher', 300),
    ('Rice Cooker', 70),
    ('Ceiling Fan', 220),
    ('Water Heater', 309),
    ('Induction Cooktop', 192),
    ('Mixer Grinder', 100),
    ('Air Purifier', 153),
    ('Juicer', 157),
    ('Iron', 130),
    ('Slow Cooker', 225),
    ('Food Processor', 210),
    ('Fiction Novel', 16),
    ('Science Fiction', 19),
    ('Mystery Book', 33),
    ('Thriller Book', 21),
    ('Biography', 12),
    ('Self-Help Book', 48),
    ('Textbook', 21),
    ("Children's Book", 97),
    ('Fantasy Book', 36),
    ('History Book', 19),
    ('Cookbook', 17),
    ('Travel Guide', 87),
    ('Graphic Novel', 37),
    ('Poetry Book', 39),
    ('Philosophy Book', 37),
    ('Psychology Book', 35),
    ('Business Book', 95),
    ('Health and Fitness Book', 40),
    ('DIY Guide', 18),
    ('Encyclopedia', 18),
    ('Soccer Ball', 34),
    ('Basketball', 15),
    ('Tennis Racket', 39),
    ('Badminton Racket', 41),
    ('Cricket Bat', 35),
    ('Baseball Glove', 42),
    ('Hockey Stick', 16),
    ('Table Tennis Paddle', 68),
    ('Golf Club', 14),
    ('Yoga Mat', 21),
    ('Dumbbells', 42)
]

product_categories = ["Electronics", "Clothing", "Home Appliances", "Books", "Sports"]
payment_types = ["Debit Card", "Credit Card", "Cryptocurrency", "PayPal"]
failure_reasons = ["Insufficient Funds", "Network Issue", "Card Expired", "Other"]
countries = ["USA", "India", "UK", "Germany"]
rogue_values = ["Null", "None", " ", "-1", "!@$@", "_-", "..."]