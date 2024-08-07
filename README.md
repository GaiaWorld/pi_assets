
在了解总资产管理器之前， 我们需要先达成一个共识， 即： 所有的资产， 都可以按照如下两种方式划分：
1. 可按照是否正在使用， 分为正在使用的资产（UseingAsset）和未使用的资产（UnusedAsset）
2. 可按照类别是否相同， 分为图片（Image）、视频（Video）、音频（Audio）， Ui纹理（UiTexture），3D纹理（3DTexture）等。

资产管理器实现的**核心宗旨**， 是保证应用内的所有资产尽量不超过指定的总大小。具体来说， 即： 当所有资产的总大小超过指定总大小时， 资产管理器会自动释放部分未使用的资产， 以保证总大小不超过指定总大小。这个目标听起来似乎很简单，但要兼顾资产管理器的性能，程序的可维护性，我们需要做更复杂的事情。

以Ui纹理（UiTexture），3D纹理（3DTexture）为例。
假定资产管理器中只存在**UiTexture**，**3DTexture**两类资产， 资产管理器配置的总容量配置为200MB，当前资产管理器管理的**UiTexture**有200MB，其中有30MB是正在使用， 另外170M是未被使用，此时，在应用中打开3D场景，需要在资产管理器中加载30MB的**3DTexture**，我们希望能将**UiTexture**中未使用的170MB的部分(30MB)丢弃, 以使得资产总量尽量维持在200MB以内。
一个简单的做法是， 往资产管理器中push任何一个资产，都关心所有类别的自资产持有情况，即在push 3D资产时，要判断**UiTexture**的持有情况， 如果**UiTexture**的持有情况满足释放条件，则释放**UiTexture**，然后再push 3D资产。但是，如果不止存在两种类别的资产， 假如有30中资产类别， 最差的情况， 我们需要判断30次，才能确定应该释放哪个资产或者无资产可释放


资管管理器已经考虑过以下情形，并针对这些情形制定了**围绕资产管理器核心宗旨**的**相对合理的解决方案**, 我们以Ui纹理（UiTexture），3D纹理（3DTexture）为例，来解释一下资管管理器是如何应对这些情形的。

假定资产管理器中只存在**UiTexture**，**3DTexture**两类为例， 资产管理器配置的总容量为200MB：

### 情形1
**情形描述**：当前资产管理器管理的**UiTexture**有200MB，其中有30MB是正在使用， 另外170M是未被使用，此时，在应用中打开3D场景，需要在资产管理器中加载30MB的**3DTexture**
**应对方法**：将**UiTexture**中未使用的170MB的部分丢弃， 以使得资产总量尽量维持在200MB以内

### 情形2
**情形描述**：假定当前资产管理器管理的**UiTexture**有200MB，其中有30MB是正在使用， 另外170M是未被使用，此时，在应用中打开3D场景，需要在资产管理器中加载30MB的**3DTexture**
**应对方法**：将**UiTexture**中未使用的170MB的部分丢弃， 以使得资产总量尽量维持在200MB以内


