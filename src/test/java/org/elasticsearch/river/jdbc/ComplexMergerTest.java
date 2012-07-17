package org.elasticsearch.river.jdbc;

import org.testng.annotations.Test;

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

        System.out.println(root);
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
        
        System.out.println(root.toJSON());
    }


}
