# Instalación

Este proyecto Vagrant es base para las pruebas de instalación y ejercicios del curso.

# Comenzamos

1.  [Descargar e instalar VirtualBox (si es necesario).](https://www.virtualbox.org/wiki/Downloads)
2.  [Descargar e instalar Vagrant (si es necesario).](http://www.vagrantup.com/downloads.html)
3.  Ejecutar ```vagrant plugin install vagrant-vbguest```  para plugin de vboxsf.
3.  Ejecutar ```vagrant up```  para crear la maquina virtual.
4.  Ejecutar ```vagrant ssh``` para ingresar en nuesta maquina virtual.
5.  Ejecutar ```vagrant destroy``` cuando quieras destruir y deshacerte de la máquina virtual.



### Acceso remoto
Mediante  ```vagrant ssh-config  > vagrant-ssh``` podemos exportar la configuracion para acceso remoto via ```ssh```.
Ejemplo  ```ssh -F vagrant-ssh default```.

#### Vagrant
[Comandos.](https://www.vagrantup.com/docs/cli/)



