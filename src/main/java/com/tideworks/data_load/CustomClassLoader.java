package com.tideworks.data_load;

public final class CustomClassLoader extends ClassLoader {
  private ClassLoader delegatingClsLdr;
  public Class<?> cls;

  public CustomClassLoader(ClassLoader parent) {
    super(parent);
    delegatingClsLdr = parent;
  }

  void setDelegatingClassLoader(ClassLoader loader) {
    delegatingClsLdr = loader;
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    if (name.startsWith("org.apache.")) {
      System.out.printf("loading class: %s%n", name);
    }
//    System.out.printf("loading class: %s%n", name);
    final Class<?> rtnCls = delegatingClsLdr.loadClass(name);
    if (name.compareTo("org.apache.avro.Schema") == 0/* && rtnCls != cls*/) {
      return cls;
//      throw new AssertionError("redefined org.apache.avro.Schema not loaded");
    }
    return rtnCls;
  }
}
