package com.valhalla.load;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * @author : LynX
 * @create 2024/2/4 16:29
 */
public class DynamicClassLoader extends URLClassLoader {
    public DynamicClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        try {
            // 首先尝试使用父类加载器来加载类
            return super.loadClass(name);
        } catch (ClassNotFoundException e) {
            // 如果父类加载器无法加载该类，再尝试使用URLClassLoader来加载
            return findClass(name);
        }
    }
}
