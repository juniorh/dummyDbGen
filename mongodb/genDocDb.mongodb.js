function genDocDb(collname, num){
	curDate = new Date();
        people = ["Liam", "Hunter", "Connor", "Jack", "Cohen", "Jaxon", "John", "Landon", "Owen", "William", "Benjamin", "Caleb", "Henry", "Lucas", "Mason", "Noah", "Alex", "Alexander", "Carter", "Charlie", "David", "Jackson", "James", "Jase", "Joseph", "Wyatt", "Austin", "Camden", "Cameron", "Emmett", "Griffin", "Harrison", "Hudson", "Jace", "Jonah", "Kingston", "Lincoln", "Marcus", "Nash", "Nathan", "Oliver", "Parker", "Ryan", "Ryder", "Seth", "Xavier", "Charles", "Clark", "Cooper", "Daniel", "Drake", "Dylan", "Edward", "Eli", "Elijah", "Emerson", "Evan", "Felix", "Gabriel", "Gavin", "Gus", "Isaac", "Isaiah", "Jacob", "Jax", "Kai", "Kaiden", "Malcolm", "Michael", "Nathaniel", "Riley", "Sawyer", "Thomas", "Tristan", "Antonio", "Beau", "Beckett", "Brayden", "Bryce", "Caden", "Casey", "Cash", "Chase", "Clarke", "Dawson", "Declan", "Dominic", "Drew", "Elliot", "Elliott", "Ethan", "Ezra", "Gage", "Grayson", "Hayden", "Jaxson", "Jayden", "Kole", "Levi", "Logan", "Luke", "Matthew", "Morgan", "Nate", "Nolan", "Peter", "Ryker", "Sebastian", "Simon", "Tanner", "Taylor", "Theo", "Turner", "Ty", "Tye"];
        for(var i=0; i<num; i++){
                nameLen = Math.floor(Math.random() * (5 - 1)) + 1 //3-8 name langth
                name = people[Math.floor(Math.random()*people.length)];
                for(var j=0; j<nameLen; j++){
                        name = name + " " + people[Math.floor(Math.random()*people.length)];
                }
                user_id = i;
                boolean = [true, false][Math.floor(Math.random()*2)];
                added_at = new Date();
                zipcode = Math.floor(Math.random()*100001);
                phone = Math.floor(Math.random()*100000001);
                db[collname].insert({"name":name, "user_id":user_id, "boolean": boolean, "added_at":added_at, "phone":phone, "zipcode": zipcode });
		if((new Date())-curDate > 1000){
	                print(i);
			curDate = new Date();
		}
        }
}


