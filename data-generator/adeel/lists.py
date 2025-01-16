product_dict = {
    # Electronics
    "Smartphone": 50, "Tablet": 40, "Laptop": 50, "Desktop Computer": 30, "Smartwatch": 35,
    "Bluetooth Speaker": 25, "Noise-Cancelling Headphones": 30, "Gaming Console": 45,
    "External Hard Drive": 20, "USB Flash Drive": 15, "Portable Power Bank": 25,
    "Wireless Mouse": 20, "Wireless Keyboard": 20, "HDMI Cable": 10, "VR Headset": 30,
    "Drone Camera": 25, "Digital Camera": 20, "Fitness Tracker": 30, "4K TV": 40,
    "Smart Home Hub": 35, "Smart Light Bulb": 15, "Streaming Device": 30, "Car Dash Cam": 20,
    "E-Reader": 25, "Electric Scooter": 35,
    
    # Clothing
    "Cotton T-Shirt": 40, "Denim Jeans": 35, "Leather Jacket": 30, "Running Shoes": 50,
    "Backpack": 30, "Baseball Cap": 25, "Winter Coat": 30, "Yoga Pants": 35, "Swimwear": 20,
    "Formal Suit": 15, "Sneakers": 50, "Leather Belt": 20, "Silk Scarf": 10, "Sunglasses": 35,
    "Wool Sweater": 20, "Compression Socks": 15, "Summer Hat": 25, "Rain Jacket": 20,
    "Leather Gloves": 15, "Wristwatch": 30,
    
    # Home Appliances
    "Air Purifier": 20, "Robot Vacuum Cleaner": 25, "Electric Kettle": 15, "Microwave Oven": 30,
    "Coffee Maker": 35, "Blender": 25, "Food Processor": 20, "Dishwasher": 30,
    "Toaster": 20, "Steam Iron": 15, "Electric Grill": 20, "Washing Machine": 25,
    "Refrigerator": 40, "Room Heater": 15, "Water Dispenser": 15, "Portable Air Conditioner": 20,
    "Electric Fan": 20, "Vacuum Cleaner": 25, "Rice Cooker": 15, "Slow Cooker": 15,
    
    # Toys
    "Lego Set": 40, "Action Figure": 25, "Dollhouse": 25, "Remote Control Car": 30,
    "Puzzle Game": 20, "Board Game": 30, "Toy Kitchen Set": 25, "Water Gun": 15,
    "Plush Toy": 20, "Building Blocks": 30, "Stuffed Animal": 20, "Card Game": 15,
    "Rubik’s Cube": 25, "Electric Train Set": 20, "PlayDough Set": 20, "Dinosaur Toy": 25,
    "Toy Robot": 30, "Drawing Kit": 20, "Musical Toy": 20, "Toy Camera": 15,
    
    # Books
    "Fantasy Novel": 20, "Science Fiction Book": 15, "Self-Help Guide": 25, "Cookbook": 30,
    "Historical Biography": 15, "Travel Guide": 20, "Children's Storybook": 30,
    "Mystery Novel": 25, "Poetry Collection": 10, "Language Learning Book": 15,
    "Photography Manual": 10, "DIY Home Improvement Guide": 20, "Business Strategy Book": 15,
    "Fitness & Nutrition Guide": 20, "Inspirational Memoir": 15, "Thriller Novel": 25,
    "Graphic Novel": 15, "Classic Literature": 10, "Romantic Novel": 20, "Adventure Book": 25,
    
    # Tools
    "Cordless Drill": 30, "Hammer": 20, "Screwdriver Set": 25, "Measuring Tape": 20,
    "Power Saw": 25, "Level Tool": 15, "Toolbox": 25, "Wrench Set": 20, "Paint Roller": 10,
    "Electric Sander": 20, "Socket Set": 20, "Nail Gun": 15, "Hand Saw": 15,
    "Utility Knife": 15, "Caulking Gun": 10, "Drill Bits Set": 20, "Work Gloves": 15,
    "Safety Goggles": 10, "Ladder": 15, "Extension Cord": 10,
    
    # Sporting Goods
    "Camping Tent": 30, "Sleeping Bag": 25, "Yoga Mat": 30, "Electric Guitar": 20,
    "Folding Chair": 20, "Hiking Backpack": 25, "Mountain Bike": 30, "Trekking Poles": 15,
    "Portable Camping Stove": 25, "Fishing Rod": 20, "Football": 30, "Basketball": 30,
    "Tennis Racket": 25, "Swimming Goggles": 20, "Skateboard": 25, "Dumbbell Set": 30,
    "Resistance Bands": 20, "Punching Bag": 20, "Golf Clubs": 25, "Knee Pads": 15
}

# General Price List
# Variation Based on Retailer
# AMAZON -3%
# WALMART 0%
# TARGET +3%
price_dict = {
    # Electronics
    "Smartphone": 799.99, "Tablet": 599.99, "Laptop": 999.99, "Desktop Computer": 1200.00,
    "Smartwatch": 299.99, "Bluetooth Speaker": 149.99, "Noise-Cancelling Headphones": 249.99,
    "Gaming Console": 499.99, "External Hard Drive": 89.99, "USB Flash Drive": 1299.99,
    "Portable Power Bank": 39.99, "Wireless Mouse": 69.99, "Wireless Keyboard": 49.99,
    "HDMI Cable": 14.99, "VR Headset": 399.99, "Drone Camera": 999.99, "Digital Camera": 699.99,
    "Fitness Tracker": 199.99, "4K TV": 799.99, "Smart Home Hub": 249.99,
    "Smart Light Bulb": 19.99, "Streaming Device": 49.99, "Car Dash Cam": 119.99,
    "E-Reader": 89.99, "Electric Scooter": 799.99,
    
    # Clothing & Accessories
    "Cotton T-Shirt": 19.99, "Denim Jeans": 49.99, "Leather Jacket": 199.99,
    "Running Shoes": 89.99, "Backpack": 59.99, "Baseball Cap": 19.99, "Winter Coat": 149.99,
    "Yoga Pants": 29.99, "Swimwear": 19.99, "Formal Suit": 299.99, "Sneakers": 99.99,
    "Leather Belt": 29.99, "Silk Scarf": 14.99, "Sunglasses": 49.99, "Wool Sweater": 39.99,
    "Compression Socks": 12.99, "Summer Hat": 15.99, "Rain Jacket": 19.99,
    "Leather Gloves": 24.99, "Wristwatch": 149.99,
    
    # Home Appliances
    "Air Purifier": 149.99, "Robot Vacuum Cleaner": 499.99, "Electric Kettle": 29.99,
    "Microwave Oven": 129.99, "Coffee Maker": 199.99, "Blender": 149.99,
    "Food Processor": 249.99, "Dishwasher": 399.99, "Toaster": 39.99,
    "Steam Iron": 99.99, "Electric Grill": 79.99, "Washing Machine": 299.99,
    "Refrigerator": 599.99, "Room Heater": 249.99, "Water Dispenser": 49.99,
    "Portable Air Conditioner": 99.99, "Electric Fan": 69.99, "Vacuum Cleaner": 199.99,
    "Rice Cooker": 89.99, "Slow Cooker": 59.99,
    
    # Toys & Games
    "Lego Set": 39.99, "Action Figure": 19.99, "Dollhouse": 49.99, "Remote Control Car": 79.99,
    "Puzzle Game": 29.99, "Board Game": 39.99, "Toy Kitchen Set": 24.99,
    "Water Gun": 19.99, "Plush Toy": 14.99, "Building Blocks": 19.99,
    "Stuffed Animal": 15.99, "Card Game": 24.99, "Rubik’s Cube": 9.99,
    "Electric Train Set": 44.99, "PlayDough Set": 12.99, "Dinosaur Toy": 34.99,
    "Toy Robot": 49.99, "Drawing Kit": 29.99, "Musical Toy": 11.99,
    "Toy Camera": 9.99,
    
    # Books
    "Fantasy Novel": 15.99, "Science Fiction Book": 24.99, "Self-Help Guide": 19.99,
    "Cookbook": 34.99, "Historical Biography": 24.99, "Travel Guide": 14.99,
    "Children's Storybook": 19.99, "Mystery Novel": 29.99, "Poetry Collection": 14.99,
    "Language Learning Book": 29.99, "Photography Manual": 19.99,
    "DIY Home Improvement Guide": 24.99, "Business Strategy Book": 14.99,
    "Fitness & Nutrition Guide": 29.99, "Inspirational Memoir": 19.99,
    "Thriller Novel": 24.99, "Graphic Novel": 29.99, "Classic Literature": 19.99,
    "Romantic Novel": 14.99, "Adventure Book": 29.99,
    
    # Tools & DIY
    "Cordless Drill": 199.99, "Hammer": 129.99, "Screwdriver Set": 49.99,
    "Measuring Tape": 99.99, "Power Saw": 299.99, "Level Tool": 59.99,
    "Toolbox": 99.99, "Wrench Set": 39.99, "Paint Roller": 129.99,
    "Electric Sander": 79.99, "Socket Set": 49.99, "Nail Gun": 29.99,
    "Hand Saw": 99.99, "Utility Knife": 79.99, "Caulking Gun": 69.99,
    "Drill Bits Set": 59.99, "Work Gloves": 29.99, "Safety Goggles": 19.99,
    "Ladder": 49.99, "Extension Cord": 19.99,
    
    # Sports & Outdoor Gear
    "Camping Tent": 199.99, "Sleeping Bag": 89.99, "Yoga Mat": 34.99,
    "Electric Guitar": 499.99, "Folding Chair": 29.99, "Hiking Backpack": 129.99,
    "Mountain Bike": 349.99, "Trekking Poles": 39.99, "Portable Camping Stove": 49.99,
    "Fishing Rod": 59.99, "Football": 24.99, "Basketball": 29.99,
    "Tennis Racket": 119.99, "Swimming Goggles": 19.99, "Skateboard": 49.99,
    "Dumbbell Set": 79.99, "Resistance Bands": 24.99, "Punching Bag": 199.99,
    "Golf Clubs": 299.99, "Knee Pads": 19.99
}


retailers = ["Amazon", "Walmart", "Target"]

payment_type = ["Card", "Bank Transfer", "Apple Pay", "Paypal"]

countries = ["USA", "Germany", "UK", "Japan", "India"]
usa_cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
germany_cities = ["Berlin", "Hamburg", "Munich", "Cologne", "Frankfurt"]
uk_cities = ["London", "Birmingham", "Manchester", "Glasgow", "Leeds"]
japan_cities = ["Tokyo", "Yokohama", "Osaka", "Nagoya", "Sapporo"]
india_cities = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Ahmedabad"]

payment_failure_reason = [
    "Insufficent Funds", "Incorrect Inforamtion", "Fraud", "Technical Issues"
]

product_categories = {
    "Electronics": list(range(1001, 1026)), 
    "Clothing": list(range(1026, 1046)),    
    "Home Appliances": list(range(1046, 1066)),
    "Toys": list(range(1066, 1086)),        
    "Books": list(range(1086, 1106)),        
    "Tools": list(range(1106, 1126)),       
    "Sporting Goods": list(range(1126, 1146)) 
}

# Holiday weights
holiday_weights = {
    "11-29": {"Electronics": 30, "Toys": 20},
    "12-25": {"Toys": 40, "Home Appliances": 20, "Books": 15}, 
    "01-01": {"Books": 20, "Fitness & Nutrition Guide": 25}, 
    "10-31": {"Toys": 30}, 
    "07-04": {"Sporting Goods": 25},  
    "10-24": {"Clothing": 25} 
}

season_weights = {
    "Winter": {"Clothing": 30, "Home Appliances": 20, "Books": 15},
    "Spring": {"Sporting Goods": 25, "Toys": 20},
    "Summer": {"Sporting Goods": 30, "Clothing": 20},
    "Fall": {"Books": 25, "Home Appliances": 15}
}