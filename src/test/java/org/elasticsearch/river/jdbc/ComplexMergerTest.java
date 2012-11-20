package org.elasticsearch.river.jdbc;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class ComplexMergerTest {
    private ComplexMerger merger = new ComplexMerger();

    @Test
    public void testRun()throws Exception{


        ComplexMerger.PropertyRoot root = merger.run("a.b","bonjour");

        merger.merge(root,"a.b","au revoir");

        System.out.println(root);


    }

    @Test
    public void testSquareBrackets()throws Exception{
        ComplexMerger.PropertyRoot root = merger.createRoot();

        merger.merge(root,"a.c","Titre");
        merger.merge(root,"a.b[id]","Bonjour");
        merger.merge(root,"a.b[label]","Au revoir");
        merger.merge(root,"a.b[id]","Hi");
        merger.merge(root,"a.b[label]","Bye");

        System.out.println(root);
    }


    @Test
    public void testComplexSquareBrackets()throws Exception{
        ComplexMerger.PropertyRoot root = merger.createRoot();

        merger.merge(root,"a.title","Titre");
        merger.merge(root,"a.b[c.id]","Bonjour");
        merger.merge(root,"a.b[c.label]","Au revoir");
        merger.merge(root,"a.b[c.id]","Hi");
        merger.merge(root,"a.b[c.label]","Bye");
        merger.merge(root,"a.b[d.id]","Manger");
        merger.merge(root,"a.b[d.label]","Dormir");
        merger.merge(root,"a.b[d.id]","Eat");
        merger.merge(root,"a.b[d.label]","Sleep");

        System.out.println(root.getXBuilder().string());

        ComplexMerger.PropertyRoot root2 = merger.createRoot();

        merger.merge(root2,"a.title","Titre");
        merger.merge(root2,"a.b[id]","Bonjour");
        merger.merge(root2,"a.b[label]","Au revoir");
        merger.merge(root2,"a.b[id]","Hi");
        merger.merge(root2,"a.b[label]","Bye");
        merger.merge(root2,"a.b[id]","Manger");
        merger.merge(root2,"a.b[label]","Dormir");
        merger.merge(root2,"a.b[id]","Eat");
        merger.merge(root2,"a.b[label]","Sleep");

        System.out.println(root2.getXBuilder().string());
    }

    @Test
    public void testOffre()throws Exception{
        ComplexMerger.PropertyRoot root = merger.createRoot();

        merger.merge(root,"_id","12");
        merger.merge(root,"offre.id","12");
        merger.merge(root,"offre.title","Titre");
        merger.merge(root,"offre.jobcategories[id]","1");
        merger.merge(root,"offre.jobcategories[label]","Fonction 1");
        merger.merge(root,"offre.jobcategories[id]","2");
        merger.merge(root,"offre.jobcategories[label]","Fonction 2");
        merger.merge(root,"offre.jobcategories[id]","3");
        merger.merge(root,"offre.jobcategories[label]","Fonction 3");
        merger.merge(root,"offre.industries[id]","12");
        merger.merge(root,"offre.industries[label]","Secteur 12");
        merger.merge(root,"offre.industries[id]","15");
        merger.merge(root,"offre.industries[label]","Secteur 15");
        merger.merge(root,"offre.howToApply.reference","370");
        merger.merge(root,"offre.howToApply.url","http://bob.com");
        merger.merge(root,"offre.howToApply.contact.name","Bob marley");
        merger.merge(root,"offre.howToApply.contact.email","bob.marley@bob.com");
        merger.merge(root,"offre.howToApply.contact.url","http://www.bob.com");
        merger.merge(root,"offre.howToApply.contact.tel","01.01.01.01.01");
        merger.merge(root,"offre.howToApply.contact.address.locality","Paris");
        merger.merge(root,"offre.howToApply.contact.address.postalCode","75001");


        Assert.assertTrue(root.containsNode("_id"));

        System.out.println(root.getXBuilder().string());
    }

    @Test
    public void testSubObject()throws Exception{
        ComplexMerger.PropertyRoot root = merger.createRoot();

        merger.merge(root,"_id","12");
        merger.merge(root,"offre.id","12");
        merger.merge(root,"offre.title","Titre");
        merger.merge(root,"offre.categories[title]","Title categories");
        merger.merge(root,"offre.categories[job.id]","1");
        merger.merge(root,"offre.categories[job.label]","Fonction 1");
        merger.merge(root,"offre.categories[title]","Deuxieme categorie");
        merger.merge(root,"offre.categories[job.id]","12");
        merger.merge(root,"offre.categories[job.label]","Fonction 12");
        merger.merge(root,"offre.industries[id]","12");
        merger.merge(root,"offre.industries[label]","Secteur 12");
        merger.merge(root,"offre.industries[id]","15");
        merger.merge(root,"offre.industries[label]","Secteur 15");


        Assert.assertTrue(root.containsNode("_id"));

        System.out.println(root.getXBuilder().string());
    }

    @Test
    public void testMerger()throws Exception{
        List<String> keys = new ArrayList<String>();
        List<Object> values = new ArrayList<Object>();
        
        keys.add("offre.id");
        keys.add("offre.title");
        keys.add("offre.jobcategories[id]");
        keys.add("offre.jobcategories[label]");
        keys.add("offre.jobcategories[id]");
        keys.add("offre.jobcategories[label]");
        keys.add("offre.industries[id]");
        keys.add("offre.industries[label]");
        keys.add("offre.industries[id]");
        keys.add("offre.industries[label]");
        keys.add("offre.howToApply.reference");
        keys.add("offre.howToApply.url");
        keys.add("offre.howToApply.contact.name");
        keys.add("offre.howToApply.contact.email");
        keys.add("offre.howToApply.contact.url");
        keys.add("offre.howToApply.contact.tel");
        keys.add("offre.howToApply.contact.address.locality");
        keys.add("offre.howToApply.contact.address.postalCode");

        values.add("12");
        values.add("Titre");
        values.add("1");
        values.add("Fonction 1");
        values.add("2");
        values.add("Fonction 2");
        values.add("12");
        values.add("Secteur 12");
        values.add("15");
        values.add("Secteur 15");
        values.add("370");
        values.add("http://bob.com");
        values.add("Bob marley");
        values.add("bob.marley@bob.com");
        values.add("http://www.bob.com");
        values.add("01.01.01.01.01");
        values.add("Paris");
        values.add("75001");

        ComplexMerger merger = new ComplexMerger();
        merger.row("index","jobs","job","1",keys,values);
        System.out.println(merger.getRoot().getXBuilder().string());

    }

}
