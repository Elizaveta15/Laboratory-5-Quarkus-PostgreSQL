package org.acme;

import jakarta.transaction.Transactional;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

import java.sql.*;
import java.util.ArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.annotations.Blocking;

@ApplicationScoped
@Path("/animal")
public class AnimalResource {
    private final ArrayList<Animal> animals = new ArrayList<>();
    Connection con;

    public AnimalResource() throws Exception {
        animals.add(new Animal(10L, "Mouse"));
        animals.add(new Animal(11L, "Dog"));
        animals.add(new Animal(12L, "Cat"));

        con = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/mydb",
                "elizaveta",
                "admin202");
    }

    @Incoming("requests")
    @Outgoing("answers")
    @Blocking
    public Animal process(String request) throws InterruptedException {
        System.out.println("The search is underway");
        Thread.sleep(2000);
        Animal animal_a = animals.stream()
                .filter(animal -> request.equals(animal.getName()))
                .findFirst()
                .orElse(new Animal((long) -1, "Not found"));
        System.out.println("The search is completed");
        return animal_a;
    }


    @GET
    @Path("/main")
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {
        return "About animals";
    }

    /**
     * CREATE from database
     */
    @Transactional
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/createByName")
    public String createByName(Animal animal) throws SQLException {
        /*
        String query = "INSERT INTO animal (name, type) VALUES ('" + animal.getName() + "', '" + animal.getType() + "')";
        con.createStatement().executeUpdate(query);
        */
        animal.setId(null);
        animal.persist();
        return "Create successful";
    }

    /**
     * READ from database
     */
    @GET
    @Path("/readOne/{id}")
    public Animal readOne(@PathParam("id") String id) throws SQLException {
        /*
        String query = "SELECT * FROM animal WHERE id = " + id;
        Statement statement = con.createStatement();
        ResultSet result = statement.executeQuery(query);
        while (result.next()) {
            return new Animal(result.getLong("id"),
                    result.getString("name"),
                    result.getString("type"));
        }
         */
        return Animal.findById(id);
    }

    /**
     * READ from database
     */
    @GET
    @Path("/readByName/{name}")
    public Animal readByName(@PathParam("name") String name) throws SQLException {
        /*
        String query = "SELECT * FROM animal WHERE name = '" + name + "'";
        Statement statement = con.createStatement();
        ResultSet result = statement.executeQuery(query);
        while (result.next()) {
            return new Animal(result.getLong("id"),
                    result.getString("name"),
                    result.getString("type"));
        }
         */
        return Animal.find("name", name).firstResult();
    }

    /**
     * UPDATE from database
     */
    @PUT
    @Path("/updateByName/{name}")
    @Transactional
    public String updateByName(@PathParam("name") String name) throws SQLException {
        /*
        String query = "UPDATE animal SET type = 'Fox' WHERE name = '" + name + "'";
        con.createStatement().executeUpdate(query);
         */
        Animal.update("type = 'Fox' where name = ?1", name);
        return "Update successful";
    }

    /**
     * DELETE from database
     */
    @DELETE
    @Path("/deleteByName/{name}")
    @Transactional
    public String deleteByName(@PathParam("name") String name) throws SQLException {
        /*
        String query = "DELETE FROM animal WHERE name = '" + name + "'";
        con.createStatement().executeUpdate(query);
         */
        Animal.delete("name", name);
        return "Delete successful";
    }

    /**
     * GET-query without param
     */
    @GET
    @Path("/getOne")
    public Animal getOne() {
        return new Animal(0L, "Fox");
    }

    /**
     * GET-query with PathParam
     */
    @GET
    @Path("/getOnePath/{name}")
    public Animal getOnePath(@PathParam("name") String name) {
        return new Animal(name);
    }

    /**
     * GET-query with QueryParam
     */
    @GET
    @Path("/getOneQuery")
    public Animal getOneQuery(@QueryParam("name") String name) {
        return new Animal(name);
    }

    /**
     * POST-query Object -> Object
     */
    @POST
    @Path("/postOneShowOne")
    public Animal postOneShowOne(Animal animal) {
        return new Mammal(animal);
    }

    /**
     * POST-query Object -> ListObject
     */
    @POST
    @Path("/postOneShowList")
    public ArrayList<Mammal> postOneShowList(Animal animal) {
        ArrayList<Mammal> mammals = new ArrayList<>();
        mammals.add(new Mammal(animal));
        mammals.add(new Mammal(animal));
        mammals.add(new Mammal(animal));
        return mammals;
    }

    /**
     * POST-query ListObject -> Object
     */
    @POST
    @Path("/postListShowOne")
    public Animal postListShowOne(ArrayList<Animal> animals) {
        return animals.get(1);
    }

    /**
     * POST-query ListObject -> Object from In-Memory Collection
     */
    @POST
    @Path("/postMemory")
    public ArrayList<Animal> listCollection(Animal animal) {
        animals.add(animal);
        return animals;
    }

    /**
     * PUT-query ListObject -> Object from In-Memory Collection
     */
    @PUT
    @Path("/putMemory")
    public ArrayList<Animal> putMemory(Animal animal) {
        if (animals.stream().noneMatch(item -> item.getName().equals(animal.getName()))) {
            animals.add(new Animal(animal));
        }
        return animals;
    }

    /**
     * DELETE-query ListObject -> Object from In-Memory Collection
     */
    @DELETE
    @Path("/deleteMemory")
    public ArrayList<Animal> deleteMemory(@QueryParam("id") Long id) {
        animals.removeIf(existingAnimal -> existingAnimal.getId().equals(id));
        return animals;
    }
}