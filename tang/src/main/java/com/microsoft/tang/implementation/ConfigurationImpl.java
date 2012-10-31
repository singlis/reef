package com.microsoft.tang.implementation;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.implementation.TypeHierarchy.ClassNode;
import com.microsoft.tang.implementation.TypeHierarchy.NamedParameterNode;
import com.microsoft.tang.implementation.TypeHierarchy.Node;
import com.microsoft.tang.util.MonotonicMap;
import com.microsoft.tang.util.MonotonicSet;

public class ConfigurationImpl implements Configuration {
  final TypeHierarchy namespace = new TypeHierarchy();
  final Map<ClassNode<?>, Class<?>> boundImpls = new MonotonicMap<ClassNode<?>, Class<?>>();
  final Map<ClassNode<?>, Class<ExternalConstructor<?>>> boundConstructors = new MonotonicMap<ClassNode<?>, Class<ExternalConstructor<?>>>();
  final Set<ClassNode<?>> singletons = new MonotonicSet<ClassNode<?>>();
  final Map<NamedParameterNode<?>, String> namedParameters = new MonotonicMap<NamedParameterNode<?>, String>();
  
  // *Not* serialized.
  final Map<ClassNode<?>, Object> singletonInstances = new MonotonicMap<ClassNode<?>, Object>();
  final Map<NamedParameterNode<?>, Object> namedParameterInstances = new MonotonicMap<NamedParameterNode<?>, Object>();
  boolean sealed = false;
  boolean dirtyBit = false;

  
  public final static String REGISTERED = "registered";
  public final static String SINGLETON = "singleton";

//  ConfigurationImpl(ConfigurationBuilderImpl tang) {
//    if(dirtyBit) { throw new IllegalStateException("Can't build ConfigurationImpl from dirty ConfigurationBuilderImpl object!"); }
//    this = new ConfigurationBuilderImpl(tang);
//  }

  public InjectorImpl injector() {
    return new InjectorImpl(this);
  }

  /* (non-Javadoc)
   * @see com.microsoft.tang.implementation.Configuration#writeConfigurationFile(java.io.PrintStream)
   */
  @Override
  public void writeConfigurationFile(OutputStream o) {
    PrintStream s = new PrintStream(o);
    if (dirtyBit) {
      throw new IllegalStateException(
          "Someone called setVolatileInstance() on this ConfigurationBuilderImpl object.  Refusing to serialize it!");
    }
    Map<String, String> effectiveConfiguration = getConfiguration();
    for (String k : effectiveConfiguration.keySet()) {
      // XXX escaping of strings!!!
      s.println(k + "=" + effectiveConfiguration.get(k));
    }
  }

  /**
   * Obtain the effective configuration of this ConfigurationBuilderImpl instance. This consists
   * of string-string pairs that could be dumped directly to a Properties
   * file, for example. Currently, this method does not return information
   * about default parameter values that were specified by parameter
   * annotations, or about the auto-discovered stuff in TypeHierarchy. All of
   * that should be automatically imported as these keys are parsed on the
   * other end.
   * 
   * @return a String to String map
   */
  public Map<String, String> getConfiguration() {
    if (dirtyBit) {
      throw new IllegalStateException(
          "Someone called setVolatileInstance() on this ConfigurationBuilderImpl object; no introspection allowed!");
    }

    Map<String, String> ret = new HashMap<String, String>();
    for (Class<?> opt : namespace.registeredClasses) {
      ret.put(opt.getName(), REGISTERED);
    }
    for (Node opt : boundImpls.keySet()) {
      ret.put(opt.getFullName(), boundImpls.get(opt).getName());
    }
    for (Node opt : boundConstructors.keySet()) {
      ret.put(opt.getFullName(), boundConstructors.get(opt).getName());
    }
    for (Node opt : namedParameters.keySet()) {
      ret.put(opt.getFullName(), namedParameters.get(opt));
    }
    for (Node opt : singletons) {
      ret.put(opt.getFullName(), SINGLETON);
    }
    return ret;
  }
  static public Configuration processConfiguration(Map<String, String> conf) throws BindException, ClassNotFoundException {
    ConfigurationBuilderImpl t = new ConfigurationBuilderImpl();
    for(Entry<String,String> e : conf.entrySet()) {
      if(SINGLETON.equals(e.getValue())) {
        t.bindSingleton(Class.forName(e.getKey()));
      } else if(REGISTERED.equals(e.getValue())) {
        t.register(Class.forName(e.getKey()));
      } else {
        t.bind(e.getKey(), e.getValue());
      }
    }
    return t.build();
  }
}